#!/bin/sh
# Provisiona as filas SQS no MiniStack. É idempotente: pode rodar várias vezes.
# O endpoint e as credenciais vêm das variáveis de ambiente do serviço queue-init.
set -eu

echo "Aguardando o SQS em ${AWS_ENDPOINT_URL}..."
until aws sqs list-queues >/dev/null 2>&1; do
  sleep 2
done

# 1) DLQ (FIFO)
aws sqs create-queue \
  --queue-name wager-transactions-dlq.fifo \
  --attributes FifoQueue=true >/dev/null

DLQ_URL=$(aws sqs get-queue-url \
  --queue-name wager-transactions-dlq.fifo \
  --query QueueUrl --output text)

DLQ_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$DLQ_URL" \
  --attribute-names QueueArn \
  --query Attributes.QueueArn --output text)

# 2) Fila principal (FIFO) com redrive para a DLQ.
# maxReceiveCount=5 e VisibilityTimeout=60s são valores iniciais: ajuste e
# documente a decisão no ARCHITECTURE.md.
cat > /tmp/main-queue-attrs.json <<EOF
{"FifoQueue":"true","VisibilityTimeout":"60","RedrivePolicy":"{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"5\"}"}
EOF

aws sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes file:///tmp/main-queue-attrs.json >/dev/null

# 3) Destino dos eventos de saída publicados pela outbox (FIFO).
aws sqs create-queue \
  --queue-name wager-events.fifo \
  --attributes FifoQueue=true >/dev/null

echo "Filas criadas:"
aws sqs list-queues
