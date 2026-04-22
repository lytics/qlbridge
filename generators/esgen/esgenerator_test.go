//go:build !slow

package esgen

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lytics/qlbridge/generators/gentypes"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWalk(t *testing.T) {
	ts := time.Now()
	s := schema{
		cols: map[string]value.ValueType{
			"x": value.StringType,
		},
	}
	g := NewGenerator(ts, nil, s)

	tests := []struct {
		name    string
		filter  func(t *testing.T) *rel.FilterStatement
		asserts func(t *testing.T, res *gentypes.Payload)
	}{
		{
			name: "TestStar",
			filter: func(t *testing.T) *rel.FilterStatement {
				fs, err := rel.ParseFilterQL(`FILTER *`)
				require.NoError(t, err)
				return fs
			},
			asserts: func(t *testing.T, res *gentypes.Payload) {
				m, ok := res.Filter.(*matchall)
				assert.True(t, ok)
				assert.NotNil(t, m.MatchAll)
			},
		},
		{
			name: "TestNestedStar",
			filter: func(t *testing.T) *rel.FilterStatement {
				fs, err := rel.ParseFilterQL(`FILTER AND (*, x="bob")`)
				require.NoError(t, err)
				return fs
			},
			asserts: func(t *testing.T, res *gentypes.Payload) {
				b, ok := res.Filter.(*BoolFilter)
				assert.True(t, ok)
				require.NotNil(t, b.Occurs)
				require.Len(t, b.Occurs.Filter, 2)
				m, ok := b.Occurs.Filter[0].(*matchall)
				require.True(t, ok)
				require.NotNil(t, m)
				assert.NotNil(t, m.MatchAll)
				tm, ok := b.Occurs.Filter[1].(*term)
				require.True(t, ok)
				require.NotNil(t, tm)
				assert.Equal(t, "bob", tm.Term["x"])
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs := test.filter(t)
			p, err := g.WalkExpr(fs.Filter)
			require.NoError(t, err)
			test.asserts(t, p)
		})
	}
}

type schema struct {
	cols   map[string]value.ValueType
	fields map[string]*gentypes.FieldType // pre-built FieldTypes (e.g. for nested map fields)
}

func (s schema) Column(f string) (value.ValueType, bool) {
	if ft, ok := s.fields[f]; ok {
		return ft.Type, true
	}
	c, ok := s.cols[f]
	return c, ok
}

func (s schema) ColumnInfo(f string) (*gentypes.FieldType, bool) {
	if ft, ok := s.fields[f]; ok {
		return ft, true
	}
	c, ok := s.cols[f]
	if !ok {
		return nil, ok
	}
	return &gentypes.FieldType{
		Field:    f,
		Type:     c,
		TypeName: c.String(),
	}, true
}

// TestBetween covers the BETWEEN operator for both top-level and nested
// (map) fields. Regression test for a bug where nested BETWEEN emitted
// range/term queries on bare field names ("foo", "k") instead of the
// prefixed nested paths ("user_data.i", "user_data.k"), so no documents
// matched.
func TestBetween(t *testing.T) {
	tests := []struct {
		name string
		lhs  *gentypes.FieldType
		want string
	}{
		{
			name: "simple numeric",
			lhs:  &gentypes.FieldType{Field: "visitct", Type: value.IntType, TypeName: "int"},
			want: `{
				"bool": {
					"must": [
						{"range": {"visitct": {"gt": 5}}},
						{"range": {"visitct": {"lt": 10}}}
					]
				}
			}`,
		},
		{
			name: "nested map numeric",
			lhs: &gentypes.FieldType{
				Field:    "foo",
				Path:     "user_data",
				Prefix:   "i",
				Type:     value.MapIntType,
				TypeName: "map[string]int",
			},
			want: `{
				"nested": {
					"path": "user_data",
					"ignore_unmapped": true,
					"query": {
						"bool": {
							"must": [
								{"term": {"user_data.k": "foo"}},
								{"bool": {
									"must": [
										{"range": {"user_data.i": {"gt": 5}}},
										{"range": {"user_data.i": {"lt": 10}}}
									]
								}}
							]
						}
					}
				}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeBetween(tt.lhs, 5, 10)
			require.NoError(t, err)
			assertJSONEqual(t, tt.want, got)
		})
	}
}

// TestBetweenWalk exercises BETWEEN end-to-end through the filterql parser
// and the generator's WalkExpr, ensuring the dispatcher still reaches
// makeBetween with the correct FieldType.
func TestBetweenWalk(t *testing.T) {
	s := schema{
		cols: map[string]value.ValueType{
			"visitct": value.IntType,
		},
		fields: map[string]*gentypes.FieldType{
			"user_data.foo": {
				Field:    "foo",
				Path:     "user_data",
				Prefix:   "i",
				Type:     value.MapIntType,
				TypeName: "map[string]int",
			},
		},
	}
	g := NewGenerator(time.Now(), nil, s)

	tests := []struct {
		name   string
		filter string
		want   string
	}{
		{
			name:   "simple",
			filter: `FILTER visitct BETWEEN 5 AND 10`,
			want: `{
				"bool": {
					"must": [
						{"range": {"visitct": {"gt": 5}}},
						{"range": {"visitct": {"lt": 10}}}
					]
				}
			}`,
		},
		{
			name:   "nested",
			filter: `FILTER user_data.foo BETWEEN 5 AND 10`,
			want: `{
				"nested": {
					"path": "user_data",
					"ignore_unmapped": true,
					"query": {
						"bool": {
							"must": [
								{"term": {"user_data.k": "foo"}},
								{"bool": {
									"must": [
										{"range": {"user_data.i": {"gt": 5}}},
										{"range": {"user_data.i": {"lt": 10}}}
									]
								}}
							]
						}
					}
				}
			}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, err := rel.ParseFilterQL(tt.filter)
			require.NoError(t, err)
			p, err := g.WalkExpr(fs.Filter)
			require.NoError(t, err)
			assertJSONEqual(t, tt.want, p.Filter)
		})
	}
}

func assertJSONEqual(t *testing.T, want string, got any) {
	t.Helper()
	gotBytes, err := json.Marshal(got)
	require.NoError(t, err)
	var gotNorm, wantNorm any
	require.NoError(t, json.Unmarshal(gotBytes, &gotNorm))
	require.NoError(t, json.Unmarshal([]byte(want), &wantNorm))
	assert.Equal(t, wantNorm, gotNorm, "generated ES filter mismatch\nwant: %s\ngot:  %s", want, string(gotBytes))
}
