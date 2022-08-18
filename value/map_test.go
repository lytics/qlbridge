package value

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMapValue(t *testing.T) {
	mv := map[string]interface{}{"k1": 10}
	v := NewMapValue(mv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	mi := v.MapInt()
	assert.Equal(t, 1, len(mi))
	assert.Equal(t, int64(10), mi["k1"])
	mf := v.MapFloat()
	assert.Equal(t, 1, len(mf))
	assert.Equal(t, float64(10), mf["k1"])
	ms := v.MapString()
	assert.Equal(t, 1, len(mf))
	assert.Equal(t, "10", ms["k1"])
	v = NewMapValue(map[string]interface{}{"k1": "hello"})
	mt := v.MapTime()
	assert.Equal(t, 0, mt.Len())
	v = NewMapValue(map[string]interface{}{"k1": "now-4d"})
	mt = v.MapTime()
	assert.Equal(t, 1, mt.Len())
	assert.True(t, mt.Val()["k1"].Unix() > 10000)
}

func TestMapStringValue(t *testing.T) {
	msv := map[string]string{"k1": "10"}
	v := NewMapStringValue(msv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	lv := v.SliceValue()
	assert.Equal(t, 1, len(lv))
	mi := v.MapInt()
	assert.Equal(t, 1, mi.Len())
	assert.Equal(t, int64(10), mi.Val()["k1"])
	mf := v.MapNumber()
	assert.Equal(t, 1, mf.Len())
	assert.Equal(t, float64(10), mf.Val()["k1"])
	mb := v.MapBool()
	assert.Equal(t, 0, mb.Len())
	v = NewMapStringValue(map[string]string{"k1": "true"})
	mb = v.MapBool()
	assert.Equal(t, 1, mb.Len())
	assert.Equal(t, true, mb.Val()["k1"])
}

func TestMapIntValue(t *testing.T) {
	miv := map[string]int64{"k1": 10}
	v := NewMapIntValue(miv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	lv := v.SliceValue()
	assert.Equal(t, 1, len(lv))
	mi := v.MapInt()
	assert.Equal(t, 1, len(mi))
	assert.Equal(t, int64(10), mi["k1"])
	mf := v.MapFloat()
	assert.Equal(t, 1, len(mf))
	assert.Equal(t, float64(10), mf["k1"])

	mv := v.MapValue()
	assert.Equal(t, 1, mv.Len())
	assert.Equal(t, int64(10), mv.Val()["k1"].Value())
}

func TestMapNumberValue(t *testing.T) {
	mfv := map[string]float64{"k1": 10}
	v := NewMapNumberValue(mfv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	lv := v.SliceValue()
	assert.Equal(t, 1, len(lv))
	mi := v.MapInt()
	assert.Equal(t, 1, len(mi))
	assert.Equal(t, int64(10), mi["k1"])

	mv := v.MapValue()
	assert.Equal(t, 1, mv.Len())
	assert.Equal(t, float64(10), mv.Val()["k1"].Value())
}
func TestMapTimeValue(t *testing.T) {
	n := time.Now()
	mtv := map[string]time.Time{"k1": n}
	v := NewMapTimeValue(mtv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())

	mi := v.MapInt()
	assert.Equal(t, 1, len(mi))
	assert.True(t, CloseEnuf(float64(n.UnixNano()), float64(mi["k1"])))

	mv := v.MapValue()
	assert.Equal(t, 1, mv.Len())
	assert.Equal(t, n, mv.Val()["k1"].Value())
}
func TestMapBoolValue(t *testing.T) {

	mbv := map[string]bool{"k1": true}
	v := NewMapBoolValue(mbv)
	_, ok := v.Get("k1")
	assert.True(t, ok)
	_, ok = v.Get("nope")
	assert.Equal(t, false, ok)
	assert.Equal(t, 1, v.MapValue().Len())
	lv := v.SliceValue()
	assert.Equal(t, 1, len(lv))

	mv := v.MapValue()
	assert.Equal(t, 1, mv.Len())
	assert.Equal(t, true, mv.Val()["k1"].Value())
}
