package relay

import (
	"testing"
	"time"
)

func TestNextBackoff(t *testing.T) {
	base, maxB := 500*time.Millisecond, 4*time.Second
	var got []time.Duration
	cur := time.Duration(0)
	for range 6 {
		cur = nextBackoff(cur, base, maxB)
		got = append(got, cur)
	}
	want := []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second, 4 * time.Second, 4 * time.Second, 4 * time.Second}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("backoff sequence = %v, want %v", got, want)
		}
	}
}
