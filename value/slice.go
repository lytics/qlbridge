package value

import (
	"encoding/json"
	"strings"
)

func NewSliceValues(v []Value) SliceValue {
	return SliceValue{v: v}
}
func NewSliceValuesNative(iv []interface{}) SliceValue {
	vs := make([]Value, len(iv))
	for i, v := range iv {
		vs[i] = NewValue(v)
	}
	return SliceValue{v: vs}
}

func (m SliceValue) Nil() bool          { return len(m.v) == 0 }
func (m SliceValue) Err() bool          { return false }
func (m SliceValue) Type() ValueType    { return SliceValueType }
func (m SliceValue) Value() interface{} { return m.v }
func (m SliceValue) Val() []Value       { return m.v }
func (m SliceValue) IsZero() bool       { return m.Nil() }
func (m SliceValue) ToString() string {
	sv := make([]string, len(m.Val()))
	for i, val := range m.v {
		sv[i] = val.ToString()
	}
	return strings.Join(sv, ",")
}

func (m *SliceValue) Append(v Value)              { m.v = append(m.v, v) }
func (m SliceValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m SliceValue) Len() int                     { return len(m.v) }
func (m SliceValue) SliceValue() []Value          { return m.v }
func (m SliceValue) Values() []interface{} {
	vals := make([]interface{}, len(m.v))
	for i, v := range m.v {
		vals[i] = v.Value()
	}
	return vals
}
