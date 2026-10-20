#!/usr/bin/env bash
# Demonstração com 3 PROCESSOS independentes (as 3 instâncias do Docker
# Compose, nas portas 8081, 8082 e 8083), cada um com suas conexões e memória.
#
#   1. Duas apostas distintas de 80.00, simultâneas, em instâncias diferentes,
#      sobre uma carteira de 100.00: uma processada, uma rejeitada, saldo 20.00
#      e um único débito no ledger.
#   2. A mesma aposta enviada 50 vezes em paralelo, espalhada pelas 3
#      instâncias: um único débito; as outras 49 respostas são replays.
#   3. Reconciliação das duas carteiras: saldo armazenado = ledger.
#
# Pré-requisitos: ambiente no ar (docker compose up --build -d), curl, jq e uuidgen.
set -euo pipefail

KEYCLOAK=${KEYCLOAK:-http://localhost:8080}
PORTS=(8081 8082 8083)

# ---- conferências antes de começar: falhar cedo, com uma mensagem clara ----
missing=()
for tool in curl jq uuidgen; do
	command -v "$tool" >/dev/null || missing+=("$tool")
done
if ((${#missing[@]})); then
	echo "Faltam ferramentas: ${missing[*]}" >&2
	echo "No Ubuntu/WSL: sudo apt install curl jq uuid-runtime   |   no Fedora: sudo dnf install curl jq util-linux" >&2
	exit 1
fi
for port in "${PORTS[@]}"; do
	if ! curl -sf -o /dev/null --max-time 5 "http://localhost:$port/health/ready"; then
		echo "A instância em localhost:$port não está pronta (/health/ready)." >&2
		echo "Confira com 'docker compose ps': keycloak e as três instâncias app devem estar healthy." >&2
		exit 1
	fi
done

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

pass=0
fail=0
check() { # check <descrição> <obtido> <esperado>
	if [[ "$2" == "$3" ]]; then
		echo "  [ok]     $1: $2"
		pass=$((pass + 1))
	else
		echo "  [FALHOU] $1: obtido '$2', esperado '$3'"
		fail=$((fail + 1))
	fi
}

token() {
	curl -sf -X POST "$KEYCLOAK/realms/testetecnico/protocol/openid-connect/token" \
		-d grant_type=client_credentials -d client_id="$1" -d client_secret="$1-secret" | jq -r .access_token
}

open_wallet() { # open_wallet <saldo> -> "walletId playerId"
	local player body
	player=$(uuidgen)
	body=$(curl -sf -X POST "http://localhost:${PORTS[0]}/wallets" \
		-H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
		-d "{\"playerId\":\"$player\",\"initialBalance\":{\"amount\":\"$1\",\"currency\":\"BRL\"}}")
	echo "$(jq -r .id <<<"$body") $player"
}

bet() { # bet <porta> <walletId> <playerId> <externalId> <valor> <arquivo-de-saída>
	: >"$6" # o arquivo existe mesmo se a requisição falhar; a verificação mostra a falha
	curl -s -o "$6" -w '%{http_code}\n' -X POST "http://localhost:$1/wagering/transactions" \
		-H "Authorization: Bearer $PROVIDER" -H 'Content-Type: application/json' \
		-H "Idempotency-Key: provider-a:$4" \
		-d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"$4\",\"playerId\":\"$3\",\"walletId\":\"$2\",\"roundId\":\"round-1\",\"gameId\":\"demo\",\"kind\":\"BET\",\"money\":{\"amount\":\"$5\",\"currency\":\"BRL\"}}" \
		>"$6.code"
}

balance() {
	curl -sf "http://localhost:${PORTS[2]}/wallets/$1" -H "Authorization: Bearer $INTERNAL" | jq -r .balance.amount
}

debits() {
	curl -sf "http://localhost:${PORTS[1]}/wallets/$1/ledger?limit=200" -H "Authorization: Bearer $INTERNAL" |
		jq '[.entries[] | select(.direction == "DEBIT")] | length'
}

reconcile() {
	curl -sf -X POST "http://localhost:${PORTS[0]}/wallets/$1/reconciliation" -H "Authorization: Bearer $INTERNAL" | jq -r .consistent
}

echo "Obtendo tokens no Keycloak..."
INTERNAL=$(token internal-service) || INTERNAL=""
PROVIDER=$(token provider-a) || PROVIDER=""
if [[ -z "$INTERNAL" || "$INTERNAL" == null || -z "$PROVIDER" || "$PROVIDER" == null ]]; then
	echo "Não foi possível obter os tokens em $KEYCLOAK (o Keycloak está healthy?)." >&2
	exit 1
fi

echo
echo "1) Duas apostas simultâneas de 80.00 sobre 100.00, em instâncias diferentes"
read -r W1 P1 <<<"$(open_wallet 100.00)"
[[ -n "$W1" && "$W1" != null ]] || { echo "Não foi possível abrir a carteira." >&2; exit 1; }
RUN=$(uuidgen | cut -c1-8)
bet "${PORTS[0]}" "$W1" "$P1" "demo-$RUN-a" 80.00 "$WORK/a" &
bet "${PORTS[1]}" "$W1" "$P1" "demo-$RUN-b" 80.00 "$WORK/b" &
wait
statuses=$(jq -r .status "$WORK/a" "$WORK/b" | sort | tr '\n' ' ' | sed 's/ $//')
check "status das duas apostas" "$statuses" "PROCESSED REJECTED"
codes=$(cat "$WORK/a.code" "$WORK/b.code" | sort | tr '\n' ' ' | sed 's/ $//')
check "códigos HTTP" "$codes" "200 422"
check "failureCode da rejeitada" "$(jq -r 'select(.status=="REJECTED") | .failureCode' "$WORK/a" "$WORK/b")" "INSUFFICIENT_FUNDS"
check "saldo final" "$(balance "$W1")" "20.00"
check "débitos no ledger" "$(debits "$W1")" "1"

echo "   Reenviando as duas apostas (não pode mudar nada)..."
bet "${PORTS[2]}" "$W1" "$P1" "demo-$RUN-a" 80.00 "$WORK/ra" &
bet "${PORTS[2]}" "$W1" "$P1" "demo-$RUN-b" 80.00 "$WORK/rb" &
wait
check "reenvios são replays" "$(jq -r .idempotentReplay "$WORK/ra" "$WORK/rb" | sort -u)" "true"
check "saldo depois dos reenvios" "$(balance "$W1")" "20.00"

echo
echo "2) A mesma aposta 50 vezes em paralelo, nas 3 instâncias"
read -r W2 P2 <<<"$(open_wallet 100.00)"
for i in $(seq 1 50); do
	bet "${PORTS[$((i % 3))]}" "$W2" "$P2" "demo-$RUN-same" 10.00 "$WORK/s$i.json" &
done
wait
check "respostas novas (não replay)" "$(cat "$WORK"/s*.json | jq -s '[.[] | select(.idempotentReplay == false)] | length')" "1"
check "transactionIds distintos" "$(cat "$WORK"/s*.json | jq -s '[.[].transactionId] | unique | length')" "1"
check "saldo final" "$(balance "$W2")" "90.00"
check "débitos no ledger" "$(debits "$W2")" "1"

echo
echo "3) Reconciliação (saldo armazenado = ledger)"
check "carteira 1 consistente" "$(reconcile "$W1")" "true"
check "carteira 2 consistente" "$(reconcile "$W2")" "true"

echo
echo "Resultado: $pass verificações ok, $fail falharam."
[[ $fail -eq 0 ]]
