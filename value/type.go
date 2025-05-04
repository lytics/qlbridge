package value

import (
	"encoding/json"
	"time"
)

// This is the DataType system, ie string, int, etc
type ValueType uint8

const (
	// Enum values for Type system, DO NOT CHANGE the numbers, do not use iota
	NilType            ValueType = 0
	ErrorType          ValueType = 1
	UnknownType        ValueType = 2
	ValueInterfaceType ValueType = 3 // Is of type Value Interface, ie unknown
	NumberType         ValueType = 10
	IntType            ValueType = 11
	BoolType           ValueType = 12
	TimeType           ValueType = 13
	ByteSliceType      ValueType = 14
	StringType         ValueType = 20
	StringsType        ValueType = 21
	MapValueType       ValueType = 30
	MapIntType         ValueType = 31
	MapStringType      ValueType = 32
	MapNumberType      ValueType = 33
	MapBoolType        ValueType = 34
	MapTimeType        ValueType = 35
	SliceValueType     ValueType = 40
	StructType         ValueType = 50
	JsonType           ValueType = 51
)

var (
	typeToStr = map[ValueType]string{
		NilType:            "nil",
		ErrorType:          "error",
		UnknownType:        "unknown",
		ValueInterfaceType: "value",
		NumberType:         "number",
		IntType:            "int",
		BoolType:           "bool",
		TimeType:           "time",
		ByteSliceType:      "[]byte",
		StringType:         "string",
		StringsType:        "[]string",
		MapValueType:       "map[string]value",
		MapIntType:         "map[string]string",
		MapStringType:      "map[string]string",
		MapNumberType:      "map[string]number",
		MapTimeType:        "map[string]time",
		MapBoolType:        "map[string]bool",
		SliceValueType:     "[]value",
		StructType:         "struct",
		JsonType:           "json",
	}
	mapTypes = map[ValueType]bool{
		MapValueType:  true,
		MapIntType:    true,
		MapStringType: true,
		MapNumberType: true,
		MapTimeType:   true,
		MapBoolType:   true,
	}
	sliceTypes = map[ValueType]bool{
		StringsType:    true,
		SliceValueType: true,
	}
	numTypes = map[ValueType]bool{
		NumberType: true,
		IntType:    true,
	}
)

func (m ValueType) String() string {
	if s, ok := typeToStr[m]; ok {
		return s
	}
	return "invalid"
}

func (m ValueType) IsMap() bool {
	return mapTypes[m]
}

func (m ValueType) IsSlice() bool {
	return sliceTypes[m]
}

func (m ValueType) IsNumeric() bool {
	return numTypes[m]
}

func (m ValueType) Zero() Value {
	switch m {
	case NilType:
		return NewNilValue()
	case ErrorType:
		return NewErrorValue(nil)
	case ValueInterfaceType:
		return NewValue(nil)
	case NumberType:
		return NewNumberValue(0)
	case IntType:
		return NewIntValue(0)
	case BoolType:
		return BoolValueFalse
	case TimeType:
		return NewTimeValue(time.Unix(0, 0).UTC())
	case ByteSliceType:
		return NewByteSliceValue([]byte{})
	case StringType:
		return EmptyStringValue
	case StringsType:
		return NewStringsValue([]string{})
	case MapValueType:
		return NewMapValue(map[string]interface{}{})
	case MapIntType:
		return NewMapIntValue(map[string]int64{})
	case MapStringType:
		return NewMapStringValue(map[string]string{})
	case MapNumberType:
		return NewMapNumberValue(map[string]float64{})
	case MapBoolType:
		return NewMapBoolValue(map[string]bool{})
	case MapTimeType:
		return NewMapTimeValue(map[string]time.Time{})
	case SliceValueType:
		return NewSliceValues([]Value{})
	case StructType:
		return NewStructValue(nil)
	case JsonType:
		return NewJsonValue(json.RawMessage{})
	}
	return nil
}
