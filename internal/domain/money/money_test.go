package money

import (
	"errors"
	"math"
	"testing"
)

func cur(t *testing.T, code string) Currency {
	t.Helper()
	c, err := NewCurrency(code)
	if err != nil {
		t.Fatalf("NewCurrency(%q): %v", code, err)
	}
	return c
}

func minor(t *testing.T, v int64, c Currency) Money {
	t.Helper()
	m, err := FromMinor(v, c)
	if err != nil {
		t.Fatalf("FromMinor(%d): %v", v, err)
	}
	return m
}

func TestParse_Valid(t *testing.T) {
	tests := []struct {
		in   string
		want int64
	}{
		{"0", 0},
		{"0.0", 0},
		{"0.00", 0},
		{"0.01", 1},
		{"25", 2500},
		{"25.5", 2550},
		{"25.50", 2550},
		{"25.05", 2505},
		{"100.00", 10000},
		{"92233720368547758.07", math.MaxInt64},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			m, err := Parse(tc.in, "BRL")
			if err != nil {
				t.Fatalf("Parse(%q) unexpected error: %v", tc.in, err)
			}
			if m.Minor() != tc.want {
				t.Errorf("Parse(%q).Minor() = %d, want %d", tc.in, m.Minor(), tc.want)
			}
			if m.Currency() != BRL {
				t.Errorf("Parse(%q).Currency() = %v, want BRL", tc.in, m.Currency())
			}
		})
	}
}

func TestParse_InvalidAmount(t *testing.T) {
	tests := []string{
		"",
		" ",
		"1.",
		".5",
		"1.234",
		"0.001",
		"-1.00",
		"-0",
		"+1.00",
		"1e3",
		"1E3",
		"NaN",
		"Infinity",
		"0x10",
		"00",
		"01.00",
		"1,50",
		"1 .00",
		"1.00 ",
		" 1.00",
		"1.00\n",
		"1..0",
		"--1",
		"\u0661\u0662", // dígitos arábico-índicos
	}
	for _, in := range tests {
		t.Run(in, func(t *testing.T) {
			_, err := Parse(in, "BRL")
			if !errors.Is(err, ErrInvalidAmount) {
				t.Errorf("Parse(%q) error = %v, want ErrInvalidAmount", in, err)
			}
		})
	}
}

func TestParse_Overflow(t *testing.T) {
	tests := []string{
		"92233720368547758.08", // um centavo acima de MaxInt64
		"92233720368547759",
		"99999999999999999999999",
	}
	for _, in := range tests {
		t.Run(in, func(t *testing.T) {
			_, err := Parse(in, "BRL")
			if !errors.Is(err, ErrOverflow) {
				t.Errorf("Parse(%q) error = %v, want ErrOverflow", in, err)
			}
		})
	}
}

func TestParse_InvalidCurrency(t *testing.T) {
	tests := []string{"", "br", "BRLL", "brl", "B1L", " BRL", "BR$"}
	for _, code := range tests {
		t.Run(code, func(t *testing.T) {
			_, err := Parse("10.00", code)
			if !errors.Is(err, ErrInvalidCurrency) {
				t.Errorf("Parse with currency %q error = %v, want ErrInvalidCurrency", code, err)
			}
		})
	}
}

func TestAmount_Formatting(t *testing.T) {
	tests := []struct {
		minor int64
		want  string
	}{
		{0, "0.00"},
		{1, "0.01"},
		{100, "1.00"},
		{2550, "25.50"},
		{-5, "-0.05"},
		{-2550, "-25.50"},
		{math.MaxInt64, "92233720368547758.07"},
		{math.MinInt64, "-92233720368547758.08"},
	}
	for _, tc := range tests {
		m := minor(t, tc.minor, BRL)
		if got := m.Amount(); got != tc.want {
			t.Errorf("Amount() for %d = %q, want %q", tc.minor, got, tc.want)
		}
	}
}

func TestAmount_RoundTrip(t *testing.T) {
	for _, v := range []int64{0, 1, 99, 100, 101, 2550, 123456789, math.MaxInt64} {
		m := minor(t, v, BRL)
		back, err := Parse(m.Amount(), "BRL")
		if err != nil {
			t.Fatalf("Parse(%q): %v", m.Amount(), err)
		}
		if back != m {
			t.Errorf("round trip of %d gave %v", v, back)
		}
	}
}

func TestAdd(t *testing.T) {
	a, _ := Parse("10.00", "BRL")
	b, _ := Parse("5.50", "BRL")

	got, err := a.Add(b)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if got.Amount() != "15.50" {
		t.Errorf("Add = %s, want 15.50", got.Amount())
	}
	// imutabilidade: os operandos não mudam
	if a.Amount() != "10.00" || b.Amount() != "5.50" {
		t.Errorf("operands changed: a=%s b=%s", a.Amount(), b.Amount())
	}
}

func TestSub_AllowsNegativeResult(t *testing.T) {
	a, _ := Parse("10.00", "BRL")
	b, _ := Parse("15.50", "BRL")

	got, err := a.Sub(b)
	if err != nil {
		t.Fatalf("Sub: %v", err)
	}
	if got.Amount() != "-5.50" {
		t.Errorf("Sub = %s, want -5.50", got.Amount())
	}
	if !got.IsNegative() {
		t.Errorf("expected negative result")
	}
}

func TestOverflow(t *testing.T) {
	max := minor(t, math.MaxInt64, BRL)
	min := minor(t, math.MinInt64, BRL)
	one := minor(t, 1, BRL)
	negOne := minor(t, -1, BRL)

	tests := []struct {
		name string
		op   func() (Money, error)
	}{
		{"max + 1", func() (Money, error) { return max.Add(one) }},
		{"min + -1", func() (Money, error) { return min.Add(negOne) }},
		{"min - 1", func() (Money, error) { return min.Sub(one) }},
		{"max - -1", func() (Money, error) { return max.Sub(negOne) }},
		{"negate min", func() (Money, error) { return min.Negate() }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.op(); !errors.Is(err, ErrOverflow) {
				t.Errorf("%s: error = %v, want ErrOverflow", tc.name, err)
			}
		})
	}
}

func TestBoundariesWithoutOverflow(t *testing.T) {
	max := minor(t, math.MaxInt64, BRL)
	min := minor(t, math.MinInt64, BRL)
	one := minor(t, 1, BRL)

	if got, err := max.Sub(one); err != nil || got.Minor() != math.MaxInt64-1 {
		t.Errorf("max - 1 = %v, %v", got, err)
	}
	if got, err := min.Add(one); err != nil || got.Minor() != math.MinInt64+1 {
		t.Errorf("min + 1 = %v, %v", got, err)
	}
	if got, err := max.Sub(max); err != nil || !got.IsZero() {
		t.Errorf("max - max = %v, %v", got, err)
	}
}

func TestNegate(t *testing.T) {
	a, _ := Parse("5.00", "BRL")
	got, err := a.Negate()
	if err != nil {
		t.Fatalf("Negate: %v", err)
	}
	if got.Amount() != "-5.00" {
		t.Errorf("Negate = %s, want -5.00", got.Amount())
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl, _ := Parse("10.00", "BRL")
	usd, _ := Parse("10.00", "USD")

	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Add error = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Sub error = %v, want ErrCurrencyMismatch", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("Cmp error = %v, want ErrCurrencyMismatch", err)
	}
}

func TestUninitialized(t *testing.T) {
	var zero Money
	valid, _ := Parse("1.00", "BRL")

	if _, err := zero.Add(valid); !errors.Is(err, ErrUninitialized) {
		t.Errorf("zero.Add error = %v, want ErrUninitialized", err)
	}
	if _, err := valid.Add(zero); !errors.Is(err, ErrUninitialized) {
		t.Errorf("valid.Add(zero) error = %v, want ErrUninitialized", err)
	}
	if _, err := zero.Negate(); !errors.Is(err, ErrUninitialized) {
		t.Errorf("zero.Negate error = %v, want ErrUninitialized", err)
	}
	if _, err := Zero(Currency{}); !errors.Is(err, ErrUninitialized) {
		t.Errorf("Zero(undefined currency) error = %v, want ErrUninitialized", err)
	}
	if _, err := FromMinor(10, Currency{}); !errors.Is(err, ErrUninitialized) {
		t.Errorf("FromMinor(undefined currency) error = %v, want ErrUninitialized", err)
	}
	if zero.IsInitialized() || zero.IsZero() || zero.IsPositive() || zero.IsNegative() {
		t.Errorf("uninitialized value must not report any sign or zero state")
	}
}

func TestCmp(t *testing.T) {
	small, _ := Parse("1.00", "BRL")
	big, _ := Parse("2.00", "BRL")

	tests := []struct {
		a, b Money
		want int
	}{
		{small, big, -1},
		{big, small, 1},
		{small, small, 0},
	}
	for _, tc := range tests {
		got, err := tc.a.Cmp(tc.b)
		if err != nil || got != tc.want {
			t.Errorf("Cmp(%s, %s) = %d, %v; want %d", tc.a, tc.b, got, err, tc.want)
		}
	}
}

func TestZeroPerCurrency(t *testing.T) {
	z, err := Zero(cur(t, "USD"))
	if err != nil {
		t.Fatalf("Zero: %v", err)
	}
	if !z.IsZero() || z.Currency().String() != "USD" || z.Amount() != "0.00" {
		t.Errorf("Zero(USD) = %v", z)
	}
}

func TestEquality(t *testing.T) {
	a, _ := Parse("25", "BRL")
	b, _ := Parse("25.00", "BRL")
	if a != b {
		t.Errorf("%v and %v should be equal after normalization", a, b)
	}
}
