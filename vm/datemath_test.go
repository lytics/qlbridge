package vm_test

import (
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/vm"
)

var _ = u.EMPTY

type dateTestCase struct {
	filter string
	match  bool
	tm     time.Time
}

func TestDateBoundaries(t *testing.T) {

	t1 := time.Now()

	evalCtx := datasource.NewContextMapTs(map[string]any{
		"last_event":           t1.Add(time.Hour * -12),
		"subscription_expires": t1.Add(time.Hour * 24 * 6),
		"lastevent":            map[string]time.Time{"signedup": t1},
		"first.event":          map[string]time.Time{"has.period": t1},
	}, true, t1)
	includeCtx := &includectx{ContextReader: evalCtx}

	tests := []dateTestCase{
		{ // false, will turn true in 12 hours
			filter: `FILTER last_event < "now-1d"`,
			match:  false,
			tm:     t1.Add(time.Hour * 12),
		},
		{ // same as previous, but swap left/right
			filter: `FILTER "now-1d" > last_event`,
			match:  false,
			tm:     t1.Add(time.Hour * 12),
		},
		{ // false, will turn true in 12 hours
			// we have a couple of different dates, so going to look at first one
			filter: `FILTER OR (
				last_event < "now-6d"
				last_event < "now-1d"
			)`,
			match: false,
			tm:    t1.Add(time.Hour * 12),
		},
		{ // This statement is true, but will turn false in 12 hours
			filter: `FILTER last_event > "now-1d"`,
			match:  true,
			tm:     t1.Add(time.Hour * 12),
		},
		{ // same as previous but swap left/right
			filter: `FILTER  "now-1d" < last_event`,
			match:  true,
			tm:     t1.Add(time.Hour * 12),
		},
		{ // false, true in 36 hours
			filter: `FILTER last_event < "now-2d"`,
			match:  false,
			tm:     t1.Add(time.Hour * 36),
		},
		{ // same as above, but swap left/right
			filter: `FILTER  "now-2d" > last_event`,
			match:  false,
			tm:     t1.Add(time.Hour * 36),
		},
		{ // same as above, but ge
			filter: `FILTER  "now-2d" >= last_event`,
			match:  false,
			tm:     t1.Add(time.Hour * 36),
		},
		{ // False, will always be false
			filter: `FILTER "now+1d" < last_event`,
			match:  false,
			tm:     time.Time{},
		},
		{ // Same as above but swap left/right
			filter: `FILTER last_event > "now+1d"`,
			match:  false,
			tm:     time.Time{},
		},
		{ // False, will always be false, le
			filter: `FILTER "now+1d" <= last_event`,
			match:  false,
			tm:     time.Time{},
		},
		{ // true, always true
			filter: `FILTER last_event < "now+1h"`,
			match:  true,
			tm:     time.Time{},
		},
		{
			filter: `FILTER "now+1h" > last_event`,
			match:  true,
			tm:     time.Time{},
		},
		{
			filter: `FILTER OR (
				"now+1h" > last_event
				x BETWEEN a AND b
				exists(not_a_field)
			)`,
			match: true,
			tm:    time.Time{},
		},
		{
			filter: `FILTER OR (
				"now+1h" > last_event
				last_event IN ("a", "b")
			)`,
			match: true,
			tm:    time.Time{},
		},
	}
	// test-todo
	// - variety of +/-
	// - between
	// - urnaryies
	// - false now, will be true in 24 hours, then exit in 48
	// - not cases
	for _, tc := range tests {
		fs := rel.MustParseFilter(tc.filter)

		// Converter to find/calculate date operations
		dc, err := vm.NewDateConverter(includeCtx, includeCtx, fs.Filter)
		require.Equal(t, nil, err)
		require.True(t, dc.HasDateMath)

		// initially we should not match
		matched, evalOk := vm.MatchesInc(includeCtx, includeCtx, fs)
		assert.True(t, evalOk, tc.filter)
		assert.Equal(t, tc.match, matched)

		// now look at boundary
		// on go 1.9 timezones being different on these two.
		require.Equal(t, tc.tm.Unix(), dc.Boundary().Unix(), tc.filter)
	}
}

func TestDateMath(t *testing.T) {

	t1 := time.Now()

	readers := []expr.ContextReader{
		datasource.NewContextMap(map[string]any{
			"event":                "login",
			"last_event":           t1,
			"signedup":             t1,
			"subscription_expires": t1.Add(time.Hour * 24 * 6),
			"lastevent":            map[string]time.Time{"signedup": t1},
			"first.event":          map[string]time.Time{"has.period": t1},
		}, true),
	}

	nc := datasource.NewNestedContextReader(readers, t1.Add(time.Minute*1))

	includeStatements := `
		FILTER signedup < "now-2d" ALIAS signedup_onedayago;
		FILTER subscription_expires < "now+1w" ALIAS subscription_expires_oneweek;
	`
	evalCtx := newIncluderCtx(nc, includeStatements)

	tests := []dateTestCase{
		{
			filter: `FILTER last_event < "now-1d"`,
			tm:     t1.Add(time.Hour * 72),
		},
		{
			filter: `FILTER AND (EXISTS event, last_event < "now-1d", INCLUDE signedup_onedayago)`,
			tm:     t1.Add(time.Hour * 72),
		},
	}
	// test-todo
	// x include w resolution
	// - variety of +/-
	// - between
	// - urnary
	// - false now, will be true in 24 hours, then exit in 48
	// - not cases
	for _, tc := range tests {
		fs := rel.MustParseFilter(tc.filter)

		// Converter to find/calculate date operations
		dc, err := vm.NewDateConverter(evalCtx, evalCtx, fs.Filter)
		require.NoError(t, err)
		require.True(t, dc.HasDateMath, tc.filter)

		// Ensure we inline/include all of the expressions
		node, err := expr.InlineIncludes(evalCtx, fs.Filter)
		assert.Equal(t, nil, err)

		// Converter to find/calculate date operations
		dc, err = vm.NewDateConverter(evalCtx, evalCtx, node)
		assert.Equal(t, nil, err)
		assert.True(t, dc.HasDateMath)

		// initially we should not match
		matched, evalOk := vm.MatchesInc(evalCtx, evalCtx, fs)
		assert.True(t, evalOk)
		assert.Equal(t, false, matched)
		/*
			// TODO:  I was trying to calculate the date in the future that
			// this filter statement would no longer be true.  BUT, need to change
			// tests to change the input event timestamp instead of this approach

			// Time at which this will match
			futureContext := newIncluderCtx(
				datasource.NewNestedContextReader(readers, tc.tm),
				includeStatements)

			matched, evalOk = vm.Matches(futureContext, fs)
			assert.True(t, evalOk)
			assert.Equal(t, true, matched, tc.filter)
		*/
	}

	fs := rel.MustParseFilter(`FILTER AND (INCLUDE not_valid_lookup)`)
	_, err := vm.NewDateConverter(evalCtx, evalCtx, fs.Filter)
	// We assume that the inclusions are preresolved
	assert.Nil(t, err)

	fs = rel.MustParseFilter(`FILTER AND ( last_event > "now-3x")`)
	_, err = vm.NewDateConverter(evalCtx, evalCtx, fs.Filter)
	assert.NotNil(t, err)

	fs = rel.MustParseFilter(`FILTER AND ( last_event == "now-")`)
	_, err = vm.NewDateConverter(evalCtx, evalCtx, fs.Filter)
	assert.NotEqual(t, nil, err)

	fs = rel.MustParseFilter(`FILTER AND ( last_event == "now+")`)
	_, err = vm.NewDateConverter(evalCtx, evalCtx, fs.Filter)
	assert.NotEqual(t, nil, err)

	fs = rel.MustParseFilter(`FILTER AND ( last_event == "now+now")`)
	_, err = vm.NewDateConverter(evalCtx, evalCtx, fs.Filter)
	assert.NotEqual(t, nil, err)

	fs = rel.MustParseFilter(`FILTER AND ( last_event == "now-3d")`)
	_, err = vm.NewDateConverter(evalCtx, evalCtx, fs.Filter)
	assert.Equal(t, nil, err)

	fs = rel.MustParseFilter(`FILTER AND ( last_event == "now")`)
	_, err = vm.NewDateConverter(evalCtx, evalCtx, fs.Filter)
	assert.Equal(t, nil, err)
}

func TestDateBoundaryForBetween(t *testing.T) {
	today := time.Now()

	type testCase struct {
		testName             string
		filter               string
		match                bool
		expectedBoundaryTime time.Time
		lastEvtTs            time.Time
	}

	tests := []testCase{
		{
			testName:             "within_window_1_day_before_lower_bound",
			filter:               `FILTER last_event BETWEEN "now-2d" AND "now+3d"`,
			match:                true,
			expectedBoundaryTime: today.AddDate(0, 0, 1), // Will exit after 1 day since lower bound is after 2 days
			lastEvtTs:            today.AddDate(0, 0, -1),
		},
		{
			testName:             "within_window_including_other_filters",
			filter:               `FILTER AND( last_event BETWEEN "now-2d" AND "now+3d", exists subscription_expires )`,
			match:                true,
			expectedBoundaryTime: today.AddDate(0, 0, 1), // Will exit after 1 day since lower bound is after 2 days
			lastEvtTs:            today.AddDate(0, 0, -1),
		},
		{
			testName:             "exact_current_time",
			filter:               `FILTER last_event BETWEEN "now-2d" AND "now+3d"`,
			match:                true,
			expectedBoundaryTime: today.AddDate(0, 0, 2), // Will exit after two days
			lastEvtTs:            today,
		},
		{
			testName:             "just_inside_lower",
			filter:               `FILTER last_event BETWEEN "now-2d" AND "now+3d"`,
			match:                true,
			expectedBoundaryTime: today.Add(time.Minute),                   // will be after a minute
			lastEvtTs:            today.AddDate(0, 0, -2).Add(time.Minute), // 2 days ago + 1 minute
		},
		{
			testName:             "just_inside_upper",
			filter:               `FILTER last_event BETWEEN "now-2d" AND "now+3d"`,
			match:                true,
			expectedBoundaryTime: today.AddDate(0, 0, 5).Add(-time.Minute), // as window is of 5 days and this one just entered
			lastEvtTs:            today.AddDate(0, 0, 3).Add(-time.Minute), // will exit after 2 days later
		},
		{
			testName:             "exact_boundary_lower",
			filter:               `FILTER last_event BETWEEN "now-2d" AND "now+3d"`,
			match:                false,                   // going to be false as it thinks it is out of window
			expectedBoundaryTime: today,                   // already in the lowerbound, so should exit now
			lastEvtTs:            today.AddDate(0, 0, -2), // Exactly 2 days ago
		},
		{
			testName:             "exact_boundary_upper",
			filter:               `FILTER last_event BETWEEN "now-2d" AND "now+3d"`,
			match:                false,                  // not entered yet
			expectedBoundaryTime: today,                  // should enter right now
			lastEvtTs:            today.AddDate(0, 0, 3), // Exactly 3 days in future
		},
		{
			testName:             "multiple_date_math",
			filter:               `FILTER AND(last_event BETWEEN "now-2d" AND "now+3d", subscription_expires > "now+1d")`,
			match:                true,
			expectedBoundaryTime: today.AddDate(0, 0, 2), // Will exit after 2 days last_event is in window and subscription_expires date is 6 days later
			lastEvtTs:            today,                  // today
		},
		{
			testName:             "not_condition",
			filter:               `FILTER NOT(last_event BETWEEN "now-2d" AND "now+3d")`,
			match:                true,
			expectedBoundaryTime: today.AddDate(0, 0, 1), // Will enter after a day as it will be inside of window
			lastEvtTs:            today.AddDate(0, 0, 4), // 4 days in the future (right of window)
		},
		{
			testName:             "multiple_between",
			filter:               `FILTER AND( last_event BETWEEN "now-2d" AND "now+3d", subscription_expires BETWEEN "now+1d" AND "now+7d")`,
			match:                true,
			expectedBoundaryTime: today.AddDate(0, 0, 1),  // Will exit after 1 day due to last_event sliding out of window after 1 day
			lastEvtTs:            today.AddDate(0, 0, -1), // 1 day ago
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			evalCtx := datasource.NewContextMapTs(map[string]interface{}{
				"last_event":           tc.lastEvtTs,
				"subscription_expires": today.Add(time.Hour * 24 * 6),
				"lastevent":            map[string]time.Time{"signedup": today},
				"first.event":          map[string]time.Time{"has.period": today},
			}, true, today)

			includeCtx := &includectx{ContextReader: evalCtx}

			fs := rel.MustParseFilter(tc.filter)

			dc, err := vm.NewDateConverterWithAnchorTime(includeCtx, includeCtx, fs.Filter, today)
			require.NoError(t, err)
			require.True(t, dc.HasDateMath)

			matched, evalOk := vm.MatchesInc(includeCtx, includeCtx, fs)
			assert.True(t, evalOk)
			assert.Equal(t, tc.match, matched)

			assert.Equal(t, tc.expectedBoundaryTime.Unix(), dc.Boundary().Unix())
		})
	}
}
