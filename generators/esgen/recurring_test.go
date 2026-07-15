//go:build !slow

package esgen

import (
	"testing"
	"time"

	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/stretchr/testify/require"
)

func TestRecurring(t *testing.T) {
	t.Parallel()

	// 2026-06-29 is a Monday (ISO day-of-week 1).
	ts := time.Date(2026, 6, 29, 12, 0, 0, 0, time.UTC)
	s := schema{cols: map[string]value.ValueType{
		"birthday": value.TimeType,
		"signup":   value.TimeType,
		"name":     value.StringType,
	}}
	g := NewGenerator(ts, nil, s)

	tests := []struct {
		name       string
		filter     string
		wantSrc    string
		wantParams map[string]any
		wantErr    bool
	}{
		{
			name:       "yearly",
			filter:     `FILTER recurring(birthday, "yearly")`,
			wantSrc:    "doc['birthday'].size() != 0 && doc['birthday'].value.getMonthValue() == params.month && doc['birthday'].value.getDayOfMonth() == params.day",
			wantParams: map[string]any{"month": 6, "day": 29},
		},
		{
			name:       "monthly",
			filter:     `FILTER recurring(birthday, "monthly")`,
			wantSrc:    "doc['birthday'].size() != 0 && doc['birthday'].value.getDayOfMonth() == params.day",
			wantParams: map[string]any{"day": 29},
		},
		{
			name:       "weekly",
			filter:     `FILTER recurring(birthday, "weekly")`,
			wantSrc:    "doc['birthday'].size() != 0 && doc['birthday'].value.getDayOfWeek().getValue() == params.dow",
			wantParams: map[string]any{"dow": 1},
		},
		{
			name:       "half-birthday yearly+182",
			filter:     `FILTER recurring(birthday, "yearly", 182)`,
			wantSrc:    "doc['birthday'].size() != 0 && doc['birthday'].value.getMonthValue() == params.month && doc['birthday'].value.getDayOfMonth() == params.day",
			wantParams: map[string]any{"month": 12, "day": 29}, // 2026-06-29 minus 182 days = 2025-12-29
		},
		{
			name:       "every 90 days",
			filter:     `FILTER recurring(signup, 90)`,
			wantSrc:    "if (doc['signup'].size() != 0) { long d = params.todayDay - (doc['signup'].value.toInstant().getEpochSecond() / 86400) - params.offset; return d >= 0 && d % params.n == 0; } return false;",
			wantParams: map[string]any{"todayDay": ts.UTC().Unix() / 86400, "offset": 0, "n": int64(90)},
		},
		{
			name:    "bad period",
			filter:  `FILTER recurring(birthday, "fortnightly")`,
			wantErr: true,
		},
		{
			name:    "non-date field",
			filter:  `FILTER recurring(name, "yearly")`,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fs, err := rel.ParseFilterQL(tc.filter)
			require.NoError(t, err)
			p, err := g.WalkExpr(fs.Filter)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			sf, ok := p.Filter.(*ScriptFilter)
			require.True(t, ok, "expected *ScriptFilter, got %T", p.Filter)
			require.Equal(t, "painless", sf.Script.Script.Lang)
			require.Equal(t, tc.wantSrc, sf.Script.Script.Source)
			require.Equal(t, tc.wantParams, sf.Script.Script.Params)
		})
	}
}
