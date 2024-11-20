package value

import "encoding/json"

func NewByteSliceValue(v []byte) ByteSliceValue {
	return ByteSliceValue{v: v}
}

func (m ByteSliceValue) Nil() bool                    { return len(m.v) == 0 }
func (m ByteSliceValue) Err() bool                    { return false }
func (m ByteSliceValue) Type() ValueType              { return ByteSliceType }
func (m ByteSliceValue) Value() interface{}           { return m.v }
func (m ByteSliceValue) Val() []byte                  { return m.v }
func (m ByteSliceValue) ToString() string             { return string(m.v) }
func (m ByteSliceValue) MarshalJSON() ([]byte, error) { return json.Marshal(m.v) }
func (m ByteSliceValue) Len() int                     { return len(m.v) }
func (m ByteSliceValue) IsZero() bool                 { return m.Nil() }
