package value

import (
	"encoding/json"
	"strconv"
)

func NewBoolValue(v bool) BoolValue {
	if v {
		return BoolValueTrue
	}
	return BoolValueFalse
}

func (m BoolValue) Nil() bool                    { return false }
func (m BoolValue) Err() bool                    { return false }
func (m BoolValue) Type() ValueType              { return BoolType }
func (m BoolValue) Value() interface{}           { return m.v }
func (m BoolValue) Val() bool                    { return m.v }
func (m BoolValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m BoolValue) ToString() string             { return strconv.FormatBool(m.v) }
func (m BoolValue) IsZero() bool                 { return m.v == false }
