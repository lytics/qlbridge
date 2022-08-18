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

func (m ValueType) String() string {
	switch m {
	case NilType:
		return "nil"
	case ErrorType:
		return "error"
	case UnknownType:
		return "unknown"
	case ValueInterfaceType:
		return "value"
	case NumberType:
		return "number"
	case IntType:
		return "int"
	case BoolType:
		return "bool"
	case TimeType:
		return "time"
	case ByteSliceType:
		return "[]byte"
	case StringType:
		return "string"
	case StringsType:
		return "[]string"
	case MapValueType:
		return "map[string]value"
	case MapIntType:
		return "map[string]int"
	case MapStringType:
		return "map[string]string"
	case MapNumberType:
		return "map[string]number"
	case MapTimeType:
		return "map[string]time"
	case MapBoolType:
		return "map[string]bool"
	case SliceValueType:
		return "[]value"
	case StructType:
		return "struct"
	case JsonType:
		return "json"
	default:
		return "invalid"
	}
}

func (m ValueType) IsMap() bool {
	switch m {
	case MapValueType, MapIntType, MapStringType, MapNumberType, MapTimeType, MapBoolType:
		return true
	}
	return false
}

func (m ValueType) IsSlice() bool {
	switch m {
	case StringsType, SliceValueType:
		return true
	}
	return false
}

func (m ValueType) IsNumeric() bool {
	switch m {
	case NumberType, IntType:
		return true
	}
	return false
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
