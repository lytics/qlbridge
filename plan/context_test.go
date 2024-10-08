package plan

import (
	"testing"

	"github.com/stretchr/testify/assert"
	//"github.com/lytics/qlbridge/plan"
)

func TestContext(t *testing.T) {
	c := &Context{}
	var planNil *Context
	planNil.Recover() // make sure we don't panic
	assert.Equal(t, true, planNil.Equal(nil))
	assert.Equal(t, false, planNil.Equal(c))
	assert.Equal(t, false, c.Equal(nil))

	selQuery := "Select 1;"
	c1 := NewContext(selQuery)
	c2 := NewContext(selQuery)
	// Should NOT be equal because the id is not the same
	assert.Equal(t, false, c1.Equal(c2))
}
