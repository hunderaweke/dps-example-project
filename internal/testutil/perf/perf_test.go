package perf

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestViolations(t *testing.T) {
	r := result{ns: 2_000, allocs: 10, rates: map[string]float64{"rows/s": 500}}

	tests := []struct {
		name   string
		budget Budget
		want   []string
	}{
		{name: "zero budget checks nothing", budget: Budget{}},
		{name: "within every limit", budget: Budget{MaxNsPerOp: 3 * time.Microsecond, MaxAllocsPerOp: 10, MinPerSec: map[string]float64{"rows/s": 400}}},
		{name: "too slow", budget: Budget{MaxNsPerOp: time.Microsecond},
			want: []string{"case: 2µs/op exceeds budget 1µs/op"}},
		{name: "too many allocs", budget: Budget{MaxAllocsPerOp: 9},
			want: []string{"case: 10 allocs/op exceeds budget 9 allocs/op"}},
		{name: "rate too low", budget: Budget{MinPerSec: map[string]float64{"rows/s": 600}},
			want: []string{"case: 500 rows/s is below budget 600 rows/s"}},
		{name: "rate not reported", budget: Budget{MinPerSec: map[string]float64{"events/s": 1}},
			want: []string{`case: no "events/s" metric reported`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, violations("case", r, tt.budget))
		})
	}
}
