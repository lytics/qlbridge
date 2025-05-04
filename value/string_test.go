package value

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestString(t *testing.T) {
	v := NewStringValue("a")
	slv := v.StringsValue()
	assert.Equal(t, 1, slv.Len())
	assert.Equal(t, "a", slv.Val()[0])

	v = NewStringValue("15.3")
	assert.Equal(t, int64(15), v.IntValue().Val())
	v = NewStringValue("15")
	assert.Equal(t, int64(15), v.IntValue().Val())

	sv := NewStringValue("25.5")
	nv := sv.NumberValue()
	assert.True(t, CloseEnuf(nv.Float(), float64(25.5)))
}

func TestStrings(t *testing.T) {
	v := NewStringsValue([]string{"a"})
	assert.Equal(t, 1, v.Len())
	assert.Equal(t, "a", v.Val()[0])
	v.Append("b")
	assert.Equal(t, 2, v.Len())
	assert.Equal(t, "b", v.Val()[1])
	v = NewStringsValue([]string{"25.1"})
	assert.Equal(t, 1, v.Len())
	assert.Equal(t, float64(25.1), v.NumberValue().Float())
	assert.Equal(t, int64(25), v.IntValue().Int())
	v.Append("b")
	assert.Equal(t, float64(25.1), v.NumberValue().Float())
	assert.Equal(t, int64(25), v.IntValue().Int())
	v = NewStringsValue(nil)
	assert.True(t, math.IsNaN(v.NumberValue().Float()))
	assert.Equal(t, int64(0), v.IntValue().Int())

	v.Append("a")
	v.Append("a")
	m := v.Set()
	assert.Equal(t, 1, len(m))
}
