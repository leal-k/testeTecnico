// Package money implementa o value object Money: valor monetário exato, em
// unidades mínimas (centavos) e com moeda, sem nenhum ponto flutuante.
//
// Representação: int64 em unidades mínimas, com escala fixa de 2 casas
// decimais. O maior valor representável é 92233720368547758.07 (math.MaxInt64
// centavos); qualquer operação que ultrapasse o intervalo de int64 retorna
// ErrOverflow.
//
// Valores negativos existem apenas como resultado de cálculos internos
// (por exemplo, Sub). Entradas externas (Parse) nunca aceitam sinal; a regra
// "saldo nunca negativo" pertence à carteira, não a este tipo.
package money

import (
	"errors"
	"fmt"
	"math"
	"regexp"
)

var (
	ErrInvalidCurrency  = errors.New("money: invalid currency")
	ErrInvalidAmount    = errors.New("money: invalid amount")
	ErrOverflow         = errors.New("money: arithmetic overflow")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrUninitialized    = errors.New("money: uninitialized value")
)

// scale é o número de unidades mínimas por unidade da moeda (2 casas).
const scale = 100

var (
	currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
	// Aceita "25", "25.5" e "25.50". Rejeita sinal, vazio, espaços, notação
	// científica, zeros à esquerda ("01.00") e mais de 2 casas decimais.
	amountPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(?:\.([0-9]{1,2}))?$`)
)

// Currency é um código ISO 4217 de três letras maiúsculas. O valor zero é
// "não inicializado"; só NewCurrency (ou as variáveis abaixo) produz moedas
// válidas.
type Currency struct {
	code string
}

// BRL é a moeda usada pelo serviço.
var BRL = Currency{code: "BRL"}

// NewCurrency valida e cria uma moeda a partir do código (ex.: "BRL").
func NewCurrency(code string) (Currency, error) {
	if !currencyPattern.MatchString(code) {
		return Currency{}, fmt.Errorf("%w: %q", ErrInvalidCurrency, code)
	}
	return Currency{code: code}, nil
}

func (c Currency) String() string { return c.code }

// IsUndefined informa se a moeda é o valor zero (não inicializada).
func (c Currency) IsUndefined() bool { return c.code == "" }

// Money é imutável: todos os campos são privados e as operações devolvem
// novos valores.
type Money struct {
	minor    int64
	currency Currency
}

// Zero devolve o valor zero na moeda informada.
func Zero(c Currency) (Money, error) {
	return FromMinor(0, c)
}

// FromMinor cria um Money a partir de unidades mínimas (uso: reidratação
// a partir do banco e testes).
func FromMinor(minor int64, c Currency) (Money, error) {
	if c.IsUndefined() {
		return Money{}, ErrUninitialized
	}
	return Money{minor: minor, currency: c}, nil
}

// Parse converte uma string decimal e um código de moeda em Money.
// É a porta de entrada para dados externos (HTTP/SQS): nunca aceita
// negativos, escala maior que 2, notação científica nem valores fora do
// intervalo de int64.
func Parse(amount string, currency string) (Money, error) {
	cur, err := NewCurrency(currency)
	if err != nil {
		return Money{}, err
	}

	match := amountPattern.FindStringSubmatch(amount)
	if match == nil {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}

	whole, ok := parseDigits(match[1])
	if !ok {
		return Money{}, fmt.Errorf("%w: %q", ErrOverflow, amount)
	}

	var frac int64
	switch d := match[2]; len(d) {
	case 1:
		frac = int64(d[0]-'0') * 10
	case 2:
		frac = int64(d[0]-'0')*10 + int64(d[1]-'0')
	}

	if whole > (math.MaxInt64-frac)/scale {
		return Money{}, fmt.Errorf("%w: %q", ErrOverflow, amount)
	}
	return Money{minor: whole*scale + frac, currency: cur}, nil
}

// parseDigits converte uma sequência de dígitos decimais em int64, sem
// estourar o intervalo.
func parseDigits(s string) (int64, bool) {
	var n int64
	for i := 0; i < len(s); i++ {
		d := int64(s[i] - '0')
		if n > (math.MaxInt64-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	return n, true
}

func (m Money) Minor() int64       { return m.minor }
func (m Money) Currency() Currency { return m.currency }

func (m Money) IsInitialized() bool { return !m.currency.IsUndefined() }
func (m Money) IsZero() bool        { return m.IsInitialized() && m.minor == 0 }
func (m Money) IsPositive() bool    { return m.IsInitialized() && m.minor > 0 }
func (m Money) IsNegative() bool    { return m.IsInitialized() && m.minor < 0 }

// Amount devolve a representação decimal canônica, com 2 casas
// (ex.: "25.00", "-0.05"). É a forma usada na serialização e no hash.
func (m Money) Amount() string {
	var abs uint64
	negative := m.minor < 0
	if negative {
		// -(minor+1)+1 evita overflow para math.MinInt64.
		abs = uint64(-(m.minor+1)) + 1
	} else {
		abs = uint64(m.minor)
	}
	s := fmt.Sprintf("%d.%02d", abs/scale, abs%scale)
	if negative {
		return "-" + s
	}
	return s
}

func (m Money) String() string {
	if !m.IsInitialized() {
		return "<uninitialized>"
	}
	return m.Amount() + " " + m.currency.String()
}

// Add soma dois valores da mesma moeda.
func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) ||
		(o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

// Sub subtrai o de m. O resultado pode ser negativo.
func (m Money) Sub(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor < 0 && m.minor > math.MaxInt64+o.minor) ||
		(o.minor > 0 && m.minor < math.MinInt64+o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

// Negate inverte o sinal.
func (m Money) Negate() (Money, error) {
	if !m.IsInitialized() {
		return Money{}, ErrUninitialized
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

// Cmp devolve -1, 0 ou 1 conforme m seja menor, igual ou maior que o.
func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	default:
		return 0, nil
	}
}

func (m Money) compatible(o Money) error {
	if !m.IsInitialized() || !o.IsInitialized() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}
