package value

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSliceValues(t *testing.T) {
	v := NewSliceValuesNative([]interface{}{"a"})
	assert.Equal(t, 1, v.Len())
	assert.Equal(t, "a", v.Val()[0].ToString())
	v.Append(NewStringValue("b"))
	assert.Equal(t, 2, v.Len())
	assert.Equal(t, "b", v.Val()[1].ToString())
	assert.Equal(t, 2, len(v.Values()))
}
