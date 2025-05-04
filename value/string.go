package value

import (
	"encoding/json"
	"strconv"
	"strings"
)

func NewStringValue(v string) StringValue {
	return StringValue{v: v}
}

func (m StringValue) Nil() bool                    { return len(m.v) == 0 }
func (m StringValue) Err() bool                    { return false }
func (m StringValue) Type() ValueType              { return StringType }
func (m StringValue) Value() interface{}           { return m.v }
func (m StringValue) Val() string                  { return m.v }
func (m StringValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m StringValue) IsZero() bool                 { return m.Nil() }

func (m StringValue) NumberValue() NumberValue {
	fv, _ := StringToFloat64(m.v)
	return NewNumberValue(fv)
}
func (m StringValue) StringsValue() StringsValue { return NewStringsValue([]string{m.v}) }
func (m StringValue) ToString() string           { return m.v }

func (m StringValue) IntValue() IntValue {
	iv, _ := ValueToInt64(m)
	return NewIntValue(iv)
}

func NewStringsValue(v []string) StringsValue {
	return StringsValue{v: v}
}

func (m StringsValue) Nil() bool                    { return len(m.v) == 0 }
func (m StringsValue) Err() bool                    { return false }
func (m StringsValue) Type() ValueType              { return StringsType }
func (m StringsValue) Value() interface{}           { return m.v }
func (m StringsValue) Val() []string                { return m.v }
func (m *StringsValue) Append(sv string)            { m.v = append(m.v, sv) }
func (m StringsValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m StringsValue) Len() int                     { return len(m.v) }
func (m StringsValue) IsZero() bool                 { return m.Nil() }
func (m StringsValue) NumberValue() NumberValue {
	if len(m.v) > 0 {
		if fv, err := strconv.ParseFloat(m.v[0], 64); err == nil {
			return NewNumberValue(fv)
		}
	}
	return NumberNaNValue
}
func (m StringsValue) IntValue() IntValue {
	// Im not confident this is valid?   array first element?
	if len(m.v) > 0 {
		iv, _ := convertStringToInt64(0, m.v[0])
		return NewIntValue(iv)
	}
	return NewIntValue(0)
}
func (m StringsValue) ToString() string  { return strings.Join(m.v, ",") }
func (m StringsValue) Strings() []string { return m.v }
func (m StringsValue) Set() map[string]struct{} {
	setvals := make(map[string]struct{})
	for _, sv := range m.v {
		setvals[sv] = EmptyStruct
	}
	return setvals
}
func (m StringsValue) SliceValue() []Value {
	vs := make([]Value, len(m.v))
	for i, v := range m.v {
		vs[i] = NewStringValue(v)
	}
	return vs
}
