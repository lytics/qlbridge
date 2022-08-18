package value

import (
	"encoding/json"
	"strconv"
	"time"
)

func NewTimeValue(v time.Time) TimeValue {
	return TimeValue{v: v}
}

func (m TimeValue) Nil() bool                    { return m.v.IsZero() }
func (m TimeValue) Err() bool                    { return false }
func (m TimeValue) Type() ValueType              { return TimeType }
func (m TimeValue) Value() interface{}           { return m.v }
func (m TimeValue) Val() time.Time               { return m.v }
func (m TimeValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m TimeValue) ToString() string             { return strconv.FormatInt(m.Int(), 10) }
func (m TimeValue) Float() float64               { return float64(m.v.In(time.UTC).UnixNano() / 1e6) }
func (m TimeValue) Int() int64                   { return m.v.In(time.UTC).UnixNano() / 1e6 }
func (m TimeValue) Time() time.Time              { return m.v }
func (m TimeValue) IsZero() bool                 { return m.v.UnixNano() == 0 }
