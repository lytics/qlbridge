package value

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeValue(t *testing.T) {
	ts := time.Now()
	v := NewTimeValue(ts)
	assert.False(t, v.Nil())
	tsInt := ts.In(time.UTC).UnixNano() / 1e6
	assert.Equal(t, tsInt, v.Int())
	assert.Equal(t, float64(tsInt), v.Float())
	assert.False(t, v.IsZero())

	{
		v := NewTimeValue(time.Unix(0, 0))
		assert.True(t, v.IsZero())
	}
	{
		v := NewTimeValue(time.Time{})
		assert.True(t, v.Nil())

	}
}
