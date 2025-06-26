package vm

import (
	u "github.com/araddon/gou"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
)

type filterql struct {
	expr.EvalContext
	expr.Includer
}

// EvalFilerSelect evaluates a FilterSelect statement from read, into write context
//
// @writeContext = Write results of projection
// @readContext  = Message input, ie evaluate for Where/Filter clause
func EvalFilterSelect(sel *rel.FilterSelect, writeContext expr.ContextWriter, readContext expr.EvalContext) (bool, bool) {

	ctx, ok := readContext.(expr.EvalIncludeContext)
	if !ok {
		ctx = &expr.IncludeContext{ContextReader: readContext}
	}
	// Check and see if we are where Guarded, which would discard the entire message
	if sel.FilterStatement != nil {

		matches, ok := Matches(ctx, sel.FilterStatement)
		if !ok {
			return false, ok
		}
		if !matches {
			return false, ok
		}
	}

	for _, col := range sel.Columns {

		if col.Guard != nil {
			ifColValue, ok := Eval(readContext, col.Guard)
			if !ok {
				u.Debugf("Could not evaluate if:  T:%T  v:%v", col.Guard, col.Guard.String())
				continue
			}
			switch ifVal := ifColValue.(type) {
			case value.BoolValue:
				if !ifVal.Val() {
					continue // filter out this col
				}
			default:
				continue
			}

		}

		v, ok := Eval(readContext, col.Expr)
		if ok {
			writeContext.Put(col, readContext, v)
		}

	}

	return true, true
}

// Matches executes a FilterQL statement against an evaluation context
// returning true if the context matches.
func MatchesInc(inc expr.Includer, cr expr.EvalContext, stmt *rel.FilterStatement) (bool, bool) {
	return matchesExpr(cr, inc, stmt.Filter)
}

// Matches executes a FilterQL statement against an evaluation context
// returning true if the context matches.
func Matches(cr expr.EvalContext, stmt *rel.FilterStatement) (bool, bool) {
	return matchesExpr(cr, nil, stmt.Filter)
}

// MatchesExpr executes a expr.Node expression against an evaluation context
// returning true if the context matches.
func MatchesExpr(cr expr.EvalContext, node expr.Node) (bool, bool) {
	return matchesExpr(cr, nil, node)
}
func MatchesExprInc(inc expr.Includer, cr expr.EvalContext, node expr.Node) (bool, bool) {
	return matchesExpr(cr, inc, node)
}

func matchesExpr(cr expr.EvalContext, includer expr.Includer, n expr.Node) (bool, bool) {
	switch exp := n.(type) {
	case *expr.IdentityNode:
		if exp.Text == "*" || exp.Text == "match_all" {
			return true, true
		}
	}
	val, ok := EvalInc(includer, cr, n)
	if !ok || val == nil {
		return false, ok
	}
	if bv, isBool := val.(value.BoolValue); isBool {
		return bv.Val(), ok
	}
	return false, true
}
