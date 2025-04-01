package filterqlvm

import (
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/filterqlvm/compiler"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/lytics/qlbridge/vm"
)

// OptimizedVM is a high-performance virtual machine for evaluating FilterQL expressions
// It compiles expressions directly to optimized Go functions without any AST walking
type OptimizedVM struct {
	compiler *compiler.DirectCompiler
}

// NewOptimizedVM creates a new optimized VM
func NewOptimizedVM() *OptimizedVM {
	return &OptimizedVM{
		compiler: compiler.NewDirectCompiler(),
	}
}

// CompileFilter compiles a FilterQL statement
func (vm *OptimizedVM) CompileFilter(filter *rel.FilterStatement) (*compiler.CompiledExpr, error) {
	return vm.compiler.CompileFilter(filter)
}

// CompileNode compiles any expression node
func (vm *OptimizedVM) CompileNode(node expr.Node) (*compiler.CompiledExpr, error) {
	return vm.compiler.Compile(node)
}

// EvalFilter evaluates a FilterQL statement against a context
func (vm *OptimizedVM) EvalFilter(filter *rel.FilterStatement, ctx expr.EvalContext) (bool, bool) {
	// Special case for match_all
	if filter.Filter == nil {
		return true, true
	}

	switch n := filter.Filter.(type) {
	case *expr.IdentityNode:
		if n.Text == "*" || n.Text == "match_all" {
			return true, true
		}
	}

	// Compile the filter (cached internally)
	compiled, err := vm.CompileFilter(filter)
	if err != nil {
		// Fall back to the standard evaluator if compilation fails
		return vm.Matches(ctx, filter)
	}

	// Run the compiled version
	result, ok := compiled.EvalFunc(ctx)
	if !ok {
		return false, false
	}

	// Convert to bool
	if bv, isBool := result.(value.BoolValue); isBool {
		return bv.Val(), true
	}

	return false, false
}

// EvalNode evaluates any expression node against a context
func (ovm *OptimizedVM) EvalNode(node expr.Node, ctx expr.EvalContext) (value.Value, bool) {
	// Compile the node (cached internally)
	compiled, err := ovm.CompileNode(node)
	if err != nil {
		// Fall back to the standard evaluator if compilation fails
		return vm.Eval(ctx, node)
	}

	// Run the compiled version
	return compiled.EvalFunc(ctx)
}

// Matches evaluates a FilterQL statement to determine if the context matches
func (ovm *OptimizedVM) Matches(ctx expr.EvalContext, stmt *rel.FilterStatement) (bool, bool) {
	return ovm.EvalFilter(stmt, ctx)
}

// MatchesInc evaluates a FilterQL statement with an includer
func (ovm *OptimizedVM) MatchesInc(inc expr.Includer, cr expr.EvalContext, stmt *rel.FilterStatement) (bool, bool) {
	evalCtx, ok := cr.(expr.EvalIncludeContext)
	if !ok {
		evalCtx = &filterql{EvalContext: cr, Includer: inc}
	}

	return ovm.EvalFilter(stmt, evalCtx)
}

// EvalFilterSelect evaluates a FilterSelect statement
func (ovm *OptimizedVM) EvalFilterSelect(sel *rel.FilterSelect, writeContext expr.ContextWriter, readContext expr.EvalContext) (bool, bool) {
	ctx, ok := readContext.(expr.EvalIncludeContext)
	if !ok {
		ctx = &expr.IncludeContext{ContextReader: readContext}
	}

	// Check filter condition
	if sel.FilterStatement != nil {
		matches, ok := ovm.Matches(ctx, sel.FilterStatement)
		if !ok || !matches {
			return false, ok
		}
	}

	// Process columns
	for _, col := range sel.Columns {
		// Check column guard
		if col.Guard != nil {
			guardResult, ok := ovm.EvalNode(col.Guard, readContext)
			if !ok {
				continue
			}

			if guardBool, ok := guardResult.(value.BoolValue); ok && !guardBool.Val() {
				continue // Skip this column
			}
		}

		// Evaluate column expression
		v, ok := ovm.EvalNode(col.Expr, readContext)
		if ok {
			writeContext.Put(col, readContext, v)
		}
	}

	return true, true
}

// filterql implementation for handling includes
type filterql struct {
	expr.EvalContext
	expr.Includer
}
