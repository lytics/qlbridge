package value

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBoolValue(t *testing.T) {
	vt := NewBoolValue(true)
	assert.Equal(t, strconv.FormatBool(true), vt)
	assert.False(t, vt.Nil())
	vf := NewBoolValue(false)
	assert.True(t, vf.IsZero())
}
