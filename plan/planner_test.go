package plan_test

import (
	"testing"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	td "github.com/lytics/qlbridge/datasource/mockcsvtestdata"
	"github.com/lytics/qlbridge/plan"
	"github.com/lytics/qlbridge/rel"
)

type plantest struct {
	q    string
	cols int
}

var planTests = []plantest{
	{"SELECT DATABASE()", 1},
}

var _ = u.EMPTY

func planStmt(t *testing.T, ctx *plan.Context) plan.Task {
	t.Helper()
	stmt, err := rel.ParseSql(ctx.Raw)
	require.NoError(t, err)
	ctx.Stmt = stmt

	planner := plan.NewPlanner(ctx)
	pln, _ := plan.WalkStmt(ctx, stmt, planner)
	//assert.True(t, err == nil) // since the FROM doesn't exist it errors
	require.NotNil(t, pln, "must have plan")
	return pln
}
func selectPlan(t *testing.T, ctx *plan.Context) *plan.Select {
	t.Helper()
	pln := planStmt(t, ctx)

	sp, ok := pln.(*plan.Select)
	require.True(t, ok, "must be *plan.Select")
	return sp
}

func TestPlans(t *testing.T) {
	for _, pt := range planTests {
		ctx := td.TestContext(pt.q)
		u.Infof("running %s for plan check", pt.q)
		p := selectPlan(t, ctx)
		assert.True(t, p != nil)

		u.Infof("%#v", ctx.Projection)
		u.Infof("cols %#v", ctx.Projection)
		if pt.cols > 0 {
			// ensure our projection has these columns
			assert.True(t, len(ctx.Projection.Proj.Columns) == pt.cols,
				"expected %d cols got %v  %#v", pt.cols, len(ctx.Projection.Proj.Columns), ctx.Projection.Proj)
		}

	}
}
