package value

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	typeToInt = map[ValueType]uint8{
		NilType:            0,
		ErrorType:          1,
		UnknownType:        2,
		ValueInterfaceType: 3,
		NumberType:         10,
		IntType:            11,
		BoolType:           12,
		TimeType:           13,
		ByteSliceType:      14,
		StringType:         20,
		StringsType:        21,
		MapValueType:       30,
		MapIntType:         31,
		MapStringType:      32,
		MapNumberType:      33,
		MapBoolType:        34,
		MapTimeType:        35,
		SliceValueType:     40,
		StructType:         50,
		JsonType:           51,
	}
)

func TestAllValueTypesDefined(t *testing.T) {
	for v := range typeToInt {
		_, ok := typeToStr[v]
		assert.True(t, ok)
	}
	for v := range typeToStr {
		_, ok := typeToInt[v]
		assert.True(t, ok)
	}
}
func TestValueTypeUint8(t *testing.T) {
	for v := range typeToStr {
		assert.Equal(t, typeToInt[v], uint8(v))
	}
}
func TestValueTypeString(t *testing.T) {
	for v, s := range typeToStr {
		assert.Equal(t, s, v.String())
	}
}

func TestIsMap(t *testing.T) {
	for v := range mapTypes {
		assert.True(t, v.IsMap())
	}
	for v := range sliceTypes {
		assert.False(t, v.IsMap())
	}
	for v := range numTypes {
		assert.False(t, v.IsMap())
	}
}

func TestIsNumeric(t *testing.T) {
	for v := range numTypes {
		assert.True(t, v.IsNumeric())
	}
	for v := range mapTypes {
		assert.False(t, v.IsNumeric())
	}
	for v := range sliceTypes {
		assert.False(t, v.IsNumeric())
	}
}

func TestIsSlice(t *testing.T) {
	for v := range sliceTypes {
		assert.True(t, v.IsSlice())
	}
	for v := range numTypes {
		assert.False(t, v.IsSlice())
	}
	for v := range mapTypes {
		assert.False(t, v.IsSlice())
	}
}

func TestZero(t *testing.T) {
	for v, s := range typeToStr {
		if s == "unknown" {
			assert.Nil(t, v.Zero())
			continue
		}
		assert.NotNilf(t, v.Zero(), "expected not nil for %v", v)
	}
}
