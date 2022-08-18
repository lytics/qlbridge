package value

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIntValue(t *testing.T) {
	v := NewIntNil()
	assert.True(t, v.Nil())
	assert.Equal(t, "", v.ToString())
	v = NewIntValue(32)
	nv := v.NumberValue()
	assert.Equal(t, nv.Int(), int64(32))
}
func TestValueNumber(t *testing.T) {
	v := NewNumberValue(math.NaN())
	_, err := json.Marshal(&v)
	assert.Equal(t, nil, err)
	v = NewNumberValue(math.Inf(1))
	_, err = json.Marshal(&v)
	assert.Equal(t, nil, err)
	v = NewNumberValue(math.Inf(-1))
	_, err = json.Marshal(&v)
	assert.Equal(t, nil, err)
}
