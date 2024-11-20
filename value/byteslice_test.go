package value

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByteSliceValue(t *testing.T) {
	s := "mybyte"
	v := NewByteSliceValue([]byte(s))
	assert.Equal(t, s, v.ToString())
	{
		v := NewByteSliceValue([]byte{})
		assert.True(t, v.Nil())
		assert.True(t, v.IsZero())
		assert.Equal(t, 0, v.Len())
	}
}
