// Value package defines the core value types (string, int, etc) for the
// qlbridge package, mostly used to provide common interfaces instead
// of reflection for virtual machine.
package value

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"time"
)

var (
	nilStruct   *emptyStruct
	EmptyStruct = struct{}{}

	NilValueVal         = NewNilValue()
	BoolValueTrue       = BoolValue{v: true}
	BoolValueFalse      = BoolValue{v: false}
	NumberNaNValue      = NewNumberValue(math.NaN())
	EmptyStringValue    = NewStringValue("")
	EmptyStringsValue   = NewStringsValue(nil)
	EmptyMapValue       = NewMapValue(nil)
	EmptyMapStringValue = NewMapStringValue(make(map[string]string))
	EmptyMapIntValue    = NewMapIntValue(make(map[string]int64))
	EmptyMapNumberValue = NewMapNumberValue(make(map[string]float64))
	EmptyMapTimeValue   = NewMapTimeValue(make(map[string]time.Time))
	EmptyMapBoolValue   = NewMapBoolValue(make(map[string]bool))
	NilStructValue      = NewStructValue(nilStruct)
	TimeZeroValue       = NewTimeValue(time.Time{})
	ErrValue            = NewErrorValue(fmt.Errorf(""))

	_ Value = (StringValue)(EmptyStringValue)

	// force some types to implement interfaces
	_ Slice = (*StringsValue)(nil)
	_ Slice = (*SliceValue)(nil)
	_ Map   = (MapValue)(EmptyMapValue)
	_ Map   = (MapIntValue)(EmptyMapIntValue)
	_ Map   = (MapStringValue)(EmptyMapStringValue)
	_ Map   = (MapNumberValue)(EmptyMapNumberValue)
	_ Map   = (MapTimeValue)(EmptyMapTimeValue)
	_ Map   = (MapBoolValue)(EmptyMapBoolValue)
)

type emptyStruct struct{}

type (
	Value interface {
		// Is this a nil/empty?
		// empty string counts as nil, empty slices/maps, nil structs.
		Nil() bool
		// Is this an error, or unable to evaluate from Vm?
		Err() bool
		Value() interface{}
		ToString() string
		Type() ValueType
		IsZero() bool
	}
	// Certain types are Numeric (Ints, Time, Number)
	NumericValue interface {
		Float() float64
		Int() int64
	}
	// Slices can always return a []Value representation and is meant to be used
	// when iterating over all items in a non-scalar value. Maps return their keys
	// as a slice.
	Slice interface {
		SliceValue() []Value
		Len() int
		json.Marshaler
	}
	// Map interface
	Map interface {
		json.Marshaler
		Len() int
		MapValue() MapValue
		Get(key string) (Value, bool)
	}
)

type (
	NumberValue struct {
		v float64
	}
	IntValue struct {
		v int64
	}
	BoolValue struct {
		v bool
	}
	StringValue struct {
		v string
	}
	TimeValue struct {
		v time.Time
	}
	StringsValue struct {
		v []string
	}
	ByteSliceValue struct {
		v []byte
	}
	SliceValue struct {
		v []Value
	}
	MapValue struct {
		v map[string]Value
	}
	MapIntValue struct {
		v map[string]int64
	}
	MapNumberValue struct {
		v map[string]float64
	}
	MapStringValue struct {
		v map[string]string
	}
	MapBoolValue struct {
		v map[string]bool
	}
	MapTimeValue struct {
		v map[string]time.Time
	}
	StructValue struct {
		v interface{}
	}
	JsonValue struct {
		v json.RawMessage
	}
	ErrorValue struct {
		v error
	}
	NilValue struct{}
)

// ValueFromString Given a string, convert to valuetype
func ValueFromString(vt string) ValueType {
	switch vt {
	case "nil", "null":
		return NilType
	case "error":
		return ErrorType
	case "unknown":
		return UnknownType
	case "value":
		return ValueInterfaceType
	case "number":
		return NumberType
	case "int":
		return IntType
	case "bool":
		return BoolType
	case "time":
		return TimeType
	case "[]byte":
		return ByteSliceType
	case "string":
		return StringType
	case "[]string":
		return StringsType
	case "map[string]value":
		return MapValueType
	case "map[string]int":
		return MapIntType
	case "map[string]string":
		return MapStringType
	case "map[string]number":
		return MapNumberType
	case "map[string]bool":
		return MapBoolType
	case "map[string]time":
		return MapTimeType
	case "[]value":
		return SliceValueType
	case "struct":
		return StructType
	case "json":
		return JsonType
	default:
		return UnknownType
	}
}

// NewValue creates a new Value type from a native Go value.
//
// Defaults to StructValue for unknown types.
func NewValue(goVal interface{}) Value {

	switch val := goVal.(type) {
	case nil:
		return NilValueVal
	case Value:
		return val
	case float64:
		return NewNumberValue(val)
	case float32:
		return NewNumberValue(float64(val))
	case *float64:
		if val == nil {
			return NewNumberNil()
		}
		return NewNumberValue(*val)
	case *float32:
		if val == nil {
			return NewNumberNil()
		}
		return NewNumberValue(float64(*val))
	case int8:
		return NewIntValue(int64(val))
	case *int8:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case int16:
		return NewIntValue(int64(val))
	case *int16:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case int:
		return NewIntValue(int64(val))
	case *int:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case int32:
		return NewIntValue(int64(val))
	case *int32:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case int64:
		return NewIntValue(int64(val))
	case *int64:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case uint8:
		return NewIntValue(int64(val))
	case *uint8:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case uint32:
		return NewIntValue(int64(val))
	case *uint32:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case uint64:
		return NewIntValue(int64(val))
	case *uint64:
		if val != nil {
			return NewIntValue(int64(*val))
		}
		return NewIntValue(0)
	case string:
		// should we return Nil?
		// if val == "null" || val == "NULL" {}
		return NewStringValue(val)
	case json.RawMessage:
		return NewJsonValue(val)
	case bool:
		return NewBoolValue(val)
	case time.Time:
		return NewTimeValue(val)
	case *time.Time:
		return NewTimeValue(*val)
	case map[string]interface{}:
		return NewMapValue(val)
	case map[string]string:
		return NewMapStringValue(val)
	case map[string]float64:
		return NewMapNumberValue(val)
	case map[string]int64:
		return NewMapIntValue(val)
	case map[string]bool:
		return NewMapBoolValue(val)
	case map[string]int:
		nm := make(map[string]int64, len(val))
		for k, v := range val {
			nm[k] = int64(v)
		}
		return NewMapIntValue(nm)
	case map[string]time.Time:
		return NewMapTimeValue(val)
	case []string:
		return NewStringsValue(val)
	// case []uint8:
	// 	return NewByteSliceValue([]byte(val))
	case []byte:
		return NewByteSliceValue(val)
	case []time.Time:
		vals := make([]Value, len(val))
		for i, v := range val {
			vals[i] = NewValue(v)
		}
		return NewSliceValues(vals)
	case []interface{}:
		if len(val) > 0 {
			switch val[0].(type) {
			case string:
				vals := make([]string, len(val))
				for i, v := range val {
					if sv, ok := v.(string); ok {
						vals[i] = sv
					} else {
						vs := make([]Value, len(val))
						for i, v := range val {
							vs[i] = NewValue(v)
						}
						return NewSliceValues(vs)
					}
				}
				return NewStringsValue(vals)
			}
		}
		vals := make([]Value, len(val))
		for i, v := range val {
			vals[i] = NewValue(v)
		}
		return NewSliceValues(vals)
	default:
		if err, isErr := val.(error); isErr {
			return NewErrorValue(err)
		}
		rv := reflect.ValueOf(goVal)
		switch rv.Kind() {
		case reflect.Slice:
			vals := make([]Value, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				e := rv.Index(i).Interface()
				vals[i] = NewValue(e)
			}
			return NewSliceValues(vals)
		}
		return NewStructValue(val)
	}
}

func NewStructValue(v interface{}) StructValue {
	return StructValue{v: v}
}

func (m StructValue) Nil() bool                    { return m.v == nil }
func (m StructValue) Err() bool                    { return false }
func (m StructValue) Type() ValueType              { return StructType }
func (m StructValue) Value() interface{}           { return m.v }
func (m StructValue) Val() interface{}             { return m.v }
func (m StructValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m StructValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m StructValue) IsZero() bool                 { return m.Nil() }

func NewJsonValue(v json.RawMessage) JsonValue {
	return JsonValue{v: v}
}

func (m JsonValue) Nil() bool                    { return m.v == nil }
func (m JsonValue) Err() bool                    { return false }
func (m JsonValue) Type() ValueType              { return JsonType }
func (m JsonValue) Value() interface{}           { return m.v }
func (m JsonValue) Val() interface{}             { return m.v }
func (m JsonValue) MarshalJSON() ([]byte, error) { return []byte(m.v), nil }
func (m JsonValue) ToString() string             { return string(m.v) }
func (m JsonValue) IsZero() bool                 { return m.Nil() }

func NewErrorValue(v error) ErrorValue {
	return ErrorValue{v: v}
}

func NewErrorValuef(v string, args ...interface{}) ErrorValue {
	return ErrorValue{v: fmt.Errorf(v, args...)}
}

func (m ErrorValue) Nil() bool                    { return false }
func (m ErrorValue) Err() bool                    { return true }
func (m ErrorValue) Type() ValueType              { return ErrorType }
func (m ErrorValue) Value() interface{}           { return m.v }
func (m ErrorValue) Val() error                   { return m.v }
func (m ErrorValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m ErrorValue) ToString() string             { return m.v.Error() }
func (m ErrorValue) IsZero() bool                 { return m.v == nil }

// ErrorValues implement Go's error interface so they can easily cross the
// VM/Go boundary.
func (m ErrorValue) Error() string { return m.v.Error() }

func NewNilValue() NilValue {
	return NilValue{}
}

func (m NilValue) Nil() bool                    { return true }
func (m NilValue) Err() bool                    { return false }
func (m NilValue) Type() ValueType              { return NilType }
func (m NilValue) Value() interface{}           { return nil }
func (m NilValue) Val() interface{}             { return nil }
func (m NilValue) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (m NilValue) ToString() string             { return "" }
func (m NilValue) IsZero() bool                 { return m.Nil() }
