package value

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

func NewMapValue(v map[string]interface{}) MapValue {
	mv := make(map[string]Value)
	for n, val := range v {
		mv[n] = NewValue(val)
	}
	return MapValue{v: mv}
}

func (m MapValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapValue) Err() bool                    { return false }
func (m MapValue) Type() ValueType              { return MapValueType }
func (m MapValue) Value() interface{}           { return m.v }
func (m MapValue) Val() map[string]Value        { return m.v }
func (m MapValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapValue) Len() int                     { return len(m.v) }
func (m MapValue) IsZero() bool                 { return m.Nil() }
func (m MapValue) MapInt() map[string]int64 {
	mv := make(map[string]int64, len(m.v))
	for n, v := range m.v {
		intVal, ok := ValueToInt64(v)
		if ok {
			mv[n] = intVal
		}
	}
	return mv
}
func (m MapValue) MapFloat() map[string]float64 {
	mv := make(map[string]float64, len(m.v))
	for n, v := range m.v {
		fv, _ := ValueToFloat64(v)
		if !math.IsNaN(fv) {
			mv[n] = fv
		}
	}
	return mv
}
func (m MapValue) MapString() map[string]string {
	mv := make(map[string]string, len(m.v))
	for n, v := range m.v {
		mv[n] = v.ToString()
	}
	return mv
}
func (m MapValue) MapValue() MapValue {
	return m
}
func (m MapValue) MapTime() MapTimeValue {
	mv := make(map[string]time.Time, len(m.v))
	for k, v := range m.v {
		t, ok := ValueToTime(v)
		if ok && !t.IsZero() {
			mv[k] = t
		}
	}
	return NewMapTimeValue(mv)
}
func (m MapValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	return v, ok
}
func NewMapStringValue(v map[string]string) MapStringValue {
	return MapStringValue{v: v}
}

func (m MapStringValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapStringValue) Err() bool                    { return false }
func (m MapStringValue) Type() ValueType              { return MapStringType }
func (m MapStringValue) Value() interface{}           { return m.v }
func (m MapStringValue) Val() map[string]string       { return m.v }
func (m MapStringValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapStringValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapStringValue) Len() int                     { return len(m.v) }
func (m MapStringValue) IsZero() bool                 { return m.Nil() }
func (m MapStringValue) MapBool() MapBoolValue {
	mb := make(map[string]bool)
	for n, sv := range m.Val() {
		b, err := strconv.ParseBool(sv)
		if err == nil {
			mb[n] = b
		}
	}
	return NewMapBoolValue(mb)
}
func (m MapStringValue) MapInt() MapIntValue {
	mi := make(map[string]int64)
	for n, sv := range m.Val() {
		iv, err := strconv.ParseInt(sv, 10, 64)
		if err == nil {
			mi[n] = iv
		}
	}
	return NewMapIntValue(mi)
}
func (m MapStringValue) MapNumber() MapNumberValue {
	mn := make(map[string]float64)
	for n, sv := range m.Val() {
		fv, err := strconv.ParseFloat(sv, 64)
		if err == nil {
			mn[n] = fv
		}
	}
	return NewMapNumberValue(mn)
}
func (m MapStringValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewStringValue(val)
	}
	return MapValue{v: mv}
}
func (m MapStringValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewStringValue(v), ok
	}
	return nil, ok
}
func (m MapStringValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewMapIntValue(v map[string]int64) MapIntValue {
	return MapIntValue{v: v}
}

func (m MapIntValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapIntValue) Err() bool                    { return false }
func (m MapIntValue) Type() ValueType              { return MapIntType }
func (m MapIntValue) Value() interface{}           { return m.v }
func (m MapIntValue) Val() map[string]int64        { return m.v }
func (m MapIntValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapIntValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapIntValue) Len() int                     { return len(m.v) }
func (m MapIntValue) MapInt() map[string]int64     { return m.v }
func (m MapIntValue) IsZero() bool                 { return m.Nil() }
func (m MapIntValue) MapFloat() map[string]float64 {
	mv := make(map[string]float64, len(m.v))
	for n, iv := range m.v {
		mv[n] = float64(iv)
	}
	return mv
}
func (m MapIntValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewIntValue(val)
	}
	return MapValue{v: mv}
}
func (m MapIntValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewIntValue(v), ok
	}
	return nil, ok
}
func (m MapIntValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewMapNumberValue(v map[string]float64) MapNumberValue {
	return MapNumberValue{v: v}
}

func (m MapNumberValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapNumberValue) Err() bool                    { return false }
func (m MapNumberValue) Type() ValueType              { return MapNumberType }
func (m MapNumberValue) Value() interface{}           { return m.v }
func (m MapNumberValue) Val() map[string]float64      { return m.v }
func (m MapNumberValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapNumberValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapNumberValue) Len() int                     { return len(m.v) }
func (m MapNumberValue) IsZero() bool                 { return m.Nil() }
func (m MapNumberValue) MapInt() map[string]int64 {
	mv := make(map[string]int64, len(m.v))
	for n, v := range m.v {
		mv[n] = int64(v)
	}
	return mv
}
func (m MapNumberValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewNumberValue(val)
	}
	return MapValue{v: mv}
}
func (m MapNumberValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewNumberValue(v), ok
	}
	return nil, ok
}
func (m MapNumberValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}

func NewMapTimeValue(v map[string]time.Time) MapTimeValue {
	return MapTimeValue{v: v}
}

func (m MapTimeValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapTimeValue) Err() bool                    { return false }
func (m MapTimeValue) Type() ValueType              { return MapTimeType }
func (m MapTimeValue) Value() interface{}           { return m.v }
func (m MapTimeValue) Val() map[string]time.Time    { return m.v }
func (m MapTimeValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapTimeValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapTimeValue) Len() int                     { return len(m.v) }
func (m MapTimeValue) IsZero() bool                 { return m.Nil() }
func (m MapTimeValue) MapInt() map[string]int64 {
	mv := make(map[string]int64, len(m.v))
	for n, v := range m.v {
		mv[n] = v.UnixNano()
	}
	return mv
}
func (m MapTimeValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewTimeValue(val)
	}
	return MapValue{v: mv}
}
func (m MapTimeValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewTimeValue(v), ok
	}
	return nil, ok
}

func NewMapBoolValue(v map[string]bool) MapBoolValue {
	return MapBoolValue{v: v}
}

func (m MapBoolValue) Nil() bool                    { return len(m.v) == 0 }
func (m MapBoolValue) Err() bool                    { return false }
func (m MapBoolValue) Type() ValueType              { return MapBoolType }
func (m MapBoolValue) Value() interface{}           { return m.v }
func (m MapBoolValue) Val() map[string]bool         { return m.v }
func (m MapBoolValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m MapBoolValue) ToString() string             { return fmt.Sprintf("%v", m.v) }
func (m MapBoolValue) Len() int                     { return len(m.v) }
func (m MapBoolValue) IsZero() bool                 { return m.Nil() }
func (m MapBoolValue) MapValue() MapValue {
	mv := make(map[string]Value)
	for n, val := range m.v {
		mv[n] = NewBoolValue(val)
	}
	return MapValue{v: mv}
}
func (m MapBoolValue) Get(key string) (Value, bool) {
	v, ok := m.v[key]
	if ok {
		return NewBoolValue(v), ok
	}
	return nil, ok
}
func (m MapBoolValue) SliceValue() []Value {
	vs := make([]Value, 0, len(m.v))
	for k := range m.v {
		vs = append(vs, NewStringValue(k))
	}
	return vs
}
