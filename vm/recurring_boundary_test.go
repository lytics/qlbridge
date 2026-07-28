package vm_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/vm"
)

func utcDay(y int, m time.Month, d int) time.Time {
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// recurringMatches is an independent copy of the recurring() predicate the
// evaluators implement, used here as an oracle for the boundary calculation.
// Kept deliberately literal, including the truncating division on the n-day path.
func recurringMatches(anchor, now time.Time, period string, n, offsetDays int) bool {
	anchorU, nowU := anchor.UTC(), now.UTC()

	if n > 0 {
		diff := nowU.Unix()/86400 - anchorU.Unix()/86400 - int64(offsetDays)
		return diff >= 0 && diff%int64(n) == 0
	}

	target := nowU.AddDate(0, 0, -offsetDays)
	switch strings.ToLower(period) {
	case "yearly":
		return target.Month() == anchorU.Month() && target.Day() == anchorU.Day()
	case "monthly":
		return target.Day() == anchorU.Day()
	case "weekly":
		return target.Weekday() == anchorU.Weekday()
	}
	return false
}

func TestRecurringBoundary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		anchor time.Time
		now    time.Time
		period string
		n      int
		offset int
		want   time.Time
	}{
		{"yearly upcoming", utcDay(1990, 6, 29), utcDay(2026, 3, 10).Add(9 * time.Hour), "yearly", 0, 0, utcDay(2026, 6, 29)},
		{"yearly today exits tomorrow", utcDay(1990, 6, 29), utcDay(2026, 6, 29).Add(9 * time.Hour), "yearly", 0, 0, utcDay(2026, 6, 30)},
		{"yearly just passed", utcDay(1990, 6, 29), utcDay(2026, 6, 30).Add(9 * time.Hour), "yearly", 0, 0, utcDay(2027, 6, 29)},
		// Feb 29 only recurs in leap years, so the wait can be four years.
		{"leap anchor skips non-leap years", utcDay(2000, 2, 29), utcDay(2025, 3, 1), "yearly", 0, 0, utcDay(2028, 2, 29)},
		{"leap anchor today", utcDay(2000, 2, 29), utcDay(2024, 2, 29).Add(9 * time.Hour), "yearly", 0, 0, utcDay(2024, 3, 1)},

		{"monthly upcoming", utcDay(1990, 6, 15), utcDay(2026, 3, 10), "monthly", 0, 0, utcDay(2026, 3, 15)},
		{"monthly today", utcDay(1990, 6, 15), utcDay(2026, 3, 15).Add(5 * time.Hour), "monthly", 0, 0, utcDay(2026, 3, 16)},
		{"monthly next month", utcDay(1990, 6, 15), utcDay(2026, 3, 20), "monthly", 0, 0, utcDay(2026, 4, 15)},
		// The 31st skips months that don't have one.
		{"monthly 31st skips february", utcDay(1990, 1, 31), utcDay(2026, 2, 10), "monthly", 0, 0, utcDay(2026, 3, 31)},

		// 2026-06-29 and 2026-07-06 are both Mondays.
		{"weekly upcoming", utcDay(2026, 6, 29), utcDay(2026, 7, 1), "weekly", 0, 0, utcDay(2026, 7, 6)},
		{"weekly today", utcDay(2026, 6, 29), utcDay(2026, 7, 6).Add(9 * time.Hour), "weekly", 0, 0, utcDay(2026, 7, 7)},

		{"every 30 days upcoming", utcDay(2026, 1, 1), utcDay(2026, 1, 15).Add(9 * time.Hour), "", 30, 0, utcDay(2026, 1, 31)},
		{"every 30 days today", utcDay(2026, 1, 1), utcDay(2026, 1, 31).Add(9 * time.Hour), "", 30, 0, utcDay(2026, 2, 1)},
		{"every 30 days before anchor", utcDay(2026, 1, 1), utcDay(2025, 12, 31).Add(9 * time.Hour), "", 30, 0, utcDay(2026, 1, 1)},

		// Every day: true from the anchor onward, so it never flips back.
		{"every 1 day after anchor never flips", utcDay(2026, 1, 1), utcDay(2026, 5, 1), "", 1, 0, time.Time{}},
		{"every 1 day before anchor", utcDay(2026, 1, 1), utcDay(2025, 12, 1), "", 1, 0, utcDay(2026, 1, 1)},

		{"unknown period has no boundary", utcDay(1990, 6, 29), utcDay(2026, 3, 10), "fortnightly", 0, 0, time.Time{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := vm.RecurringBoundary(tc.anchor, tc.now, tc.period, tc.n, tc.offset)
			assert.Equal(t, tc.want.UTC(), got.UTC())
		})
	}
}

// TestRecurringBoundaryIsExactFlipPoint walks a year and a half of "now" values
// per configuration and checks the returned boundary against the predicate
// itself: the value must be unchanged right up to the boundary and different at
// it. This is what makes the boundary safe to schedule a re-evaluation on.
func TestRecurringBoundaryIsExactFlipPoint(t *testing.T) {
	t.Parallel()

	configs := []struct {
		name   string
		anchor time.Time
		period string
		n      int
		offset int
	}{
		{"yearly", utcDay(1990, 6, 29), "yearly", 0, 0},
		{"yearly leap anchor", utcDay(2000, 2, 29), "yearly", 0, 0},
		{"yearly half-birthday", utcDay(1990, 6, 29), "yearly", 0, 182},
		{"yearly negative offset", utcDay(1990, 6, 29), "yearly", 0, -30},
		{"monthly", utcDay(1990, 6, 15), "monthly", 0, 0},
		{"monthly 31st", utcDay(1990, 1, 31), "monthly", 0, 0},
		{"monthly with offset", utcDay(1990, 6, 15), "monthly", 0, 5},
		{"weekly", utcDay(2026, 6, 29), "weekly", 0, 0},
		{"weekly with offset", utcDay(2026, 6, 29), "weekly", 0, 3},
		{"every 30 days", utcDay(2026, 1, 1), "", 30, 0},
		{"every 7 days with offset", utcDay(2026, 1, 1), "", 7, 10},
		// Pre-1970 anchor: the n-day path divides toward zero, so the oracle has
		// to share that quirk -- which it does, being a copy of the evaluator.
		{"every 90 days pre-1970 anchor", utcDay(1965, 6, 29).Add(12 * time.Hour), "", 90, 0},
	}

	for _, cfg := range configs {
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()
			// Mid-morning, so a midnight boundary is a distinct instant from "now".
			start := utcDay(2026, 1, 1).Add(9 * time.Hour)

			for i := 0; i < 550; i++ {
				now := start.AddDate(0, 0, i)
				bt := vm.RecurringBoundary(cfg.anchor, now, cfg.period, cfg.n, cfg.offset)
				if bt.IsZero() {
					continue
				}

				require.True(t, bt.After(now), "boundary %v is not after now %v", bt, now)
				require.Equal(t, bt, bt.Truncate(24*time.Hour).UTC(), "boundary %v is not a UTC midnight", bt)

				at := func(ts time.Time) bool {
					return recurringMatches(cfg.anchor, ts, cfg.period, cfg.n, cfg.offset)
				}
				justBefore := bt.Add(-time.Nanosecond)
				require.Equal(t, at(now), at(justBefore),
					"value changed before the predicted boundary: now=%v boundary=%v", now, bt)
				require.NotEqual(t, at(justBefore), at(bt),
					"value did not change at the predicted boundary: now=%v boundary=%v", now, bt)
			}
		})
	}
}

// TestRecurringBoundaryViaDateConverter covers the contract callers actually use:
// a recurring() filter must report HasDateMath so the segment gets flagged for
// re-calculation, and expose the boundary.
func TestRecurringBoundaryViaDateConverter(t *testing.T) {
	t.Parallel()

	at := utcDay(2026, 3, 10).Add(9 * time.Hour)
	evalCtx := datasource.NewContextMapTs(map[string]any{
		"birthday":   utcDay(1990, 6, 29),
		"last_event": at.Add(-12 * time.Hour),
	}, true, at)
	inc := &includectx{ContextReader: evalCtx}

	tests := []struct {
		name        string
		filter      string
		hasDateMath bool
		want        time.Time
	}{
		{"yearly", `FILTER recurring(birthday, "yearly")`, true, utcDay(2026, 6, 29)},
		{"yearly with offset", `FILTER recurring(birthday, "yearly", 10)`, true, utcDay(2026, 7, 9)},
		// 13038 days from the anchor to now, and 13038 mod 90 == 78, so the next
		// multiple of 90 lands 12 days out.
		{"every 90 days", `FILTER recurring(birthday, 90)`, true, utcDay(2026, 3, 22)},
		// Composed with datemath: the earliest boundary wins, and last_event goes
		// stale in 12 hours -- long before June.
		{
			name:        "earliest boundary wins",
			filter:      `FILTER AND ( recurring(birthday, "yearly"), last_event > "now-1d" )`,
			hasDateMath: true,
			want:        at.Add(12 * time.Hour),
		},
		// Shapes the evaluators reject must not be flagged for re-calculation.
		{"unknown period", `FILTER recurring(birthday, "fortnightly")`, false, time.Time{}},
		{"zero day period", `FILTER recurring(birthday, 0)`, false, time.Time{}},
		{"non-literal period", `FILTER recurring(birthday, some_field)`, false, time.Time{}},
		{"too few args", `FILTER recurring(birthday)`, false, time.Time{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fs := rel.MustParseFilter(tc.filter)

			dc, err := vm.NewDateConverterWithAnchorTime(inc, inc, fs.Filter, at)
			require.NoError(t, err)
			assert.Equal(t, tc.hasDateMath, dc.HasDateMath)
			assert.Equal(t, tc.want.UTC(), dc.Boundary().UTC())
		})
	}
}

// TestRecurringBoundaryNoAnchor pins that a row with no usable date yields no
// boundary rather than an error, so one profile missing a birthday doesn't stop
// the rest of the filter's boundaries from being calculated.
func TestRecurringBoundaryNoAnchor(t *testing.T) {
	t.Parallel()

	at := utcDay(2026, 3, 10).Add(9 * time.Hour)
	evalCtx := datasource.NewContextMapTs(map[string]any{
		"last_event": at.Add(-12 * time.Hour),
	}, true, at)
	inc := &includectx{ContextReader: evalCtx}

	fs := rel.MustParseFilter(`FILTER AND ( recurring(birthday, "yearly"), last_event > "now-1d" )`)
	dc, err := vm.NewDateConverterWithAnchorTime(inc, inc, fs.Filter, at)
	require.NoError(t, err)
	assert.Equal(t, at.Add(12*time.Hour).UTC(), dc.Boundary().UTC())
}
