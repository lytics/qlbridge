package value

import (
	"fmt"
	"math"
	"strconv"
)

func NewNumberValue(v float64) NumberValue {
	return NumberValue{v: v}
}
func NewNumberNil() NumberValue {
	v := NumberValue{v: math.NaN()}
	return v
}
func (m NumberValue) Nil() bool                    { return math.IsNaN(m.v) }
func (m NumberValue) Err() bool                    { return math.IsNaN(m.v) }
func (m NumberValue) Type() ValueType              { return NumberType }
func (m NumberValue) Value() interface{}           { return m.v }
func (m NumberValue) Val() float64                 { return m.v }
func (m NumberValue) MarshalJSON() ([]byte, error) { return marshalFloat(float64(m.v)) }
func (m NumberValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m NumberValue) Float() float64               { return m.v }
func (m NumberValue) Int() int64                   { return int64(m.v) }
func (m NumberValue) IsZero() bool                 { return m.v == float64(0) }

func NewIntValue(v int64) IntValue {
	return IntValue{v: v}
}

func NewIntNil() IntValue {
	v := IntValue{v: math.MinInt32}
	return v
}

func (m IntValue) Nil() bool                    { return m.v == math.MinInt32 }
func (m IntValue) Err() bool                    { return m.v == math.MinInt32 }
func (m IntValue) Type() ValueType              { return IntType }
func (m IntValue) Value() interface{}           { return m.v }
func (m IntValue) Val() int64                   { return m.v }
func (m IntValue) MarshalJSON() ([]byte, error) { return marshalFloat(float64(m.v)) }
func (m IntValue) NumberValue() NumberValue     { return NewNumberValue(float64(m.v)) }
func (m IntValue) IsZero() bool                 { return m.v == int64(0) }
func (m IntValue) ToString() string {
	if m.v == math.MinInt32 {
		return ""
	}
	return strconv.FormatInt(m.v, 10)
}
func (m IntValue) Float() float64 { return float64(m.v) }
func (m IntValue) Int() int64     { return m.v }
