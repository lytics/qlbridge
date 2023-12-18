package optimization_test

import (
	"strings"
	"testing"
	"time"

	"github.com/lytics/qlbridge/datasource"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/optimization"
	"github.com/lytics/qlbridge/rel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type includectx struct {
	expr.ContextReader
	filters map[string]*rel.FilterStatement
}

func newIncluderCtx(cr expr.ContextReader, statements string) *includectx {
	stmts := rel.MustParseFilters(statements)
	filters := make(map[string]*rel.FilterStatement, len(stmts))
	for _, stmt := range stmts {
		filters[strings.ToLower(stmt.Alias)] = stmt
	}
	return &includectx{ContextReader: cr, filters: filters}
}
func (m *includectx) Include(name string) (expr.Node, error) {
	if filter, ok := m.filters[strings.ToLower(name)]; ok {
		return filter.Filter, nil
	}
	return nil, expr.ErrNoIncluder
}

func TestOptimizeBooleanNodes(t *testing.T) {
	nc := datasource.NewNestedContextReader([]expr.ContextReader{}, time.Now())
	ctx := newIncluderCtx(nc,
		`FILTER NOT AND (
						OR (
							zip BETWEEN 1 AND 3
							city == "Peoria, IL"
						)
						*
					) ALIAS A;
			`)
	node, err := expr.ParseExpression(`AND (INCLUDE A, zip IN (1, 2, 3), city == "Peoria, IL", NOT true)`)
	require.NoError(t, err)
	sharedIncludedNodes := optimization.NewSharedIncludedNodes()
	res, err := optimization.OptimizeBooleanNodes(ctx, node, sharedIncludedNodes)
	require.NoError(t, err)
	node, err = expr.ParseExpression(`OR(INCLUDE A, count(*) > 0)`)
	require.NoError(t, err)
	res2, err := optimization.OptimizeBooleanNodes(ctx, node, sharedIncludedNodes)
	require.NoError(t, err)

	require.IsType(t, &expr.BooleanNode{}, res)
	bnode := res.(*expr.BooleanNode)
	assert.Equal(t, 4, len(bnode.Args))
	assert.Equal(t, "NOT true", bnode.Args[0].String())
	assert.Equal(t, "city == \"Peoria, IL\"", bnode.Args[1].String())
	assert.Equal(t, "zip IN (1, 2, 3)", bnode.Args[2].String())
	require.Equal(t, "INCLUDE A", bnode.Args[3].String())
	require.IsType(t, &expr.IncludeNode{}, bnode.Args[3])
	inode := bnode.Args[3].(*expr.IncludeNode)
	require.IsType(t, &expr.BooleanNode{}, inode.ExprNode)
	bnode = inode.ExprNode.(*expr.BooleanNode)
	assert.True(t, bnode.Negated())
	assert.Equal(t, 2, len(bnode.Args))
	assert.Equal(t, "*", bnode.Args[0].String())
	require.IsType(t, &expr.BooleanNode{}, bnode.Args[1])
	bnode = bnode.Args[1].(*expr.BooleanNode)
	assert.Equal(t, 2, len(bnode.Args))
	assert.Equal(t, "city == \"Peoria, IL\"", bnode.Args[0].String())
	assert.Equal(t, "zip BETWEEN 1 AND 3", bnode.Args[1].String())

	require.IsType(t, &expr.BooleanNode{}, res2)
	bnode = res2.(*expr.BooleanNode)
	assert.Equal(t, 2, len(bnode.Args))
	assert.Equal(t, "count(*) > 0", bnode.Args[0].String())
	require.Equal(t, "INCLUDE A", bnode.Args[1].String())
	require.IsType(t, &expr.IncludeNode{}, bnode.Args[1])
	assert.Same(t, res2.(*expr.BooleanNode).Args[1].(*expr.IncludeNode).ExprNode, res.(*expr.BooleanNode).Args[3].(*expr.IncludeNode).ExprNode, "should be shared via sharedIncludedNodes")
}
