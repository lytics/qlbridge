//go:build !slow

package esgen

import (
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
			p, err := g.Walk(fs)
			require.NoError(t, err)
			test.asserts(t, p)
		})
	}
}

type schema struct {
	cols map[string]value.ValueType
}

func (s schema) Column(f string) (value.ValueType, bool) {
	c, ok := s.cols[f]
	return c, ok
}

func (s schema) ColumnInfo(f string) (*gentypes.FieldType, bool) {
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
