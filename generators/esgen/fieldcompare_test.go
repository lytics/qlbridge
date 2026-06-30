//go:build !slow

package esgen

import (
	"testing"
	"time"

	"github.com/lytics/qlbridge/generators/gentypes"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/stretchr/testify/require"
)

func TestFieldToFieldCompare(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 6, 29, 0, 0, 0, 0, time.UTC)
	s := schema{cols: map[string]value.ValueType{
		"consent": value.TimeType,
		"signup":  value.TimeType,
		"clicks":  value.IntType,
		"score":   value.NumberType,
		"name":    value.StringType,
	}}
	g := NewGenerator(ts, nil, s)

	tests := []struct {
		name    string
		filter  string
		wantSrc string
		wantErr bool
	}{
		{
			name:    "time lt time",
			filter:  `FILTER consent < signup`,
			wantSrc: "doc['consent'].size() != 0 && doc['signup'].size() != 0 && doc['consent'].value.toInstant().toEpochMilli() < doc['signup'].value.toInstant().toEpochMilli()",
		},
		{
			name:    "time gt time",
			filter:  `FILTER consent > signup`,
			wantSrc: "doc['consent'].size() != 0 && doc['signup'].size() != 0 && doc['consent'].value.toInstant().toEpochMilli() > doc['signup'].value.toInstant().toEpochMilli()",
		},
		{
			name:    "int ge number",
			filter:  `FILTER clicks >= score`,
			wantSrc: "doc['clicks'].size() != 0 && doc['score'].size() != 0 && doc['clicks'].value >= doc['score'].value",
		},
		{
			name:    "type mismatch time vs number",
			filter:  `FILTER consent < score`,
			wantErr: true,
		},
		{
			name:    "string unsupported",
			filter:  `FILTER name < name`,
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
		})
	}
}

func TestFieldToFieldNestedUnsupported(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 6, 29, 0, 0, 0, 0, time.UTC)
	g := NewGenerator(ts, nil, nestedSchema{})

	fs, err := rel.ParseFilterQL(`FILTER a < b`)
	require.NoError(t, err)
	_, err = g.WalkExpr(fs.Filter)
	require.Error(t, err)
}

type nestedSchema struct{}

func (nestedSchema) Column(string) (value.ValueType, bool) { return value.TimeType, true }
func (nestedSchema) ColumnInfo(f string) (*gentypes.FieldType, bool) {
	return &gentypes.FieldType{Field: f, Type: value.TimeType, Path: "map_events", Prefix: "t"}, true
}
