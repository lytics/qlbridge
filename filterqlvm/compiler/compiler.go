package compiler

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"

	"github.com/mb0/glob"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/lex"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/lytics/qlbridge/vm"
)

// CompiledExpr represents a compiled expression that can be evaluated directly
type CompiledExpr struct {
	// Original AST node that was compiled
	Node expr.Node
	// Direct evaluation function
	EvalFunc func(ctx expr.EvalContext) (value.Value, bool)
}

// DirectCompiler generates optimized Go functions directly in memory
type DirectCompiler struct {
	cache     map[uint64]*CompiledExpr
	cacheLock sync.RWMutex
}

// NewDirectCompiler creates a new direct compiler
func NewDirectCompiler() *DirectCompiler {
	return &DirectCompiler{
		cache: make(map[uint64]*CompiledExpr),
	}
}

func (c *DirectCompiler) CompileFilter(node *rel.FilterStatement) (*CompiledExpr, error) {
	// Generate a hash for the node to use as cache key
	hash := hashFilter(node)

	// Check cache first
	c.cacheLock.RLock()
	if compiled, ok := c.cache[hash]; ok {
		c.cacheLock.RUnlock()
		return compiled, nil
	}
	c.cacheLock.RUnlock()

	// Create the compiled expression
	compiled, err := c.compileToFunc(node.Filter)
	if err != nil {
		return nil, err
	}

	// Cache the result
	c.cacheLock.Lock()
	c.cache[hash] = compiled
	c.cacheLock.Unlock()

	return compiled, nil
}

// Compile compiles an expression node into a direct evaluation function
func (c *DirectCompiler) Compile(node expr.Node) (*CompiledExpr, error) {
	// Generate a hash for the node to use as cache key
	hash := hashNode(node)

	// Check cache first
	c.cacheLock.RLock()
	if compiled, ok := c.cache[hash]; ok {
		c.cacheLock.RUnlock()
		return compiled, nil
	}
	c.cacheLock.RUnlock()

	// Create the compiled expression
	compiled, err := c.compileToFunc(node)
	if err != nil {
		return nil, err
	}

	// Cache the result
	c.cacheLock.Lock()
	c.cache[hash] = compiled
	c.cacheLock.Unlock()

	return compiled, nil
}

// compileToFunc creates a direct evaluation function for a node
func (c *DirectCompiler) compileToFunc(node expr.Node) (*CompiledExpr, error) {
	switch n := node.(type) {
	case *expr.BinaryNode:
		return c.compileBinary(n)
	case *expr.BooleanNode:
		return c.compileBoolean(n)
	case *expr.UnaryNode:
		return c.compileUnary(n)
	case *expr.IdentityNode:
		return c.compileIdentity(n)
	case *expr.NumberNode:
		return c.compileNumber(n)
	case *expr.StringNode:
		return c.compileString(n)
	case *expr.FuncNode:
		return c.compileFunc(n)
	case *expr.TriNode:
		return c.compileTernary(n)
	case *expr.ArrayNode:
		return c.compileArray(n)
	case *expr.IncludeNode:
		return c.compileInclude(n)
	case *expr.NullNode:
		return &CompiledExpr{
			Node: n,
			EvalFunc: func(ctx expr.EvalContext) (value.Value, bool) {
				return value.NewNilValue(), true
			},
		}, nil
	case *expr.ValueNode:
		return c.compileValue(n)
	default:
		return nil, fmt.Errorf("unsupported node type: %T", node)
	}
}

// compileBinary creates a direct function for binary operations
func (c *DirectCompiler) compileBinary(node *expr.BinaryNode) (*CompiledExpr, error) {
	// Compile left and right operands
	leftExpr, err := c.compileToFunc(node.Args[0])
	if err != nil {
		return nil, err
	}

	rightExpr, err := c.compileToFunc(node.Args[1])
	if err != nil {
		return nil, err
	}

	// Create a direct function based on the operator
	evalFunc := func(ctx expr.EvalContext) (value.Value, bool) {
		// Get left and right values
		left, leftOk := leftExpr.EvalFunc(ctx)

		// Short-circuit for logical operators
		switch node.Operator.T {
		case lex.TokenLogicAnd, lex.TokenAnd:
			if !leftOk {
				return nil, false
			}
			// If left is false, no need to evaluate right
			if leftBool, ok := left.(value.BoolValue); ok && !leftBool.Val() {
				return value.NewBoolValue(false), true
			}
		case lex.TokenLogicOr, lex.TokenOr:
			if leftOk {
				// If left is true, no need to evaluate right
				if leftBool, ok := left.(value.BoolValue); ok && leftBool.Val() {
					return value.NewBoolValue(true), true
				}
			}
		}

		right, rightOk := rightExpr.EvalFunc(ctx)
		// If we could not evaluate either we can shortcut
		if !leftOk && !rightOk {
			switch node.Operator.T {
			case lex.TokenLogicOr, lex.TokenOr:
				return value.NewBoolValue(false), true
			case lex.TokenEqualEqual, lex.TokenEqual:
				// We don't alllow nil == nil here bc we have a NilValue type
				// that we would use for that
				return value.NewBoolValue(false), true
			case lex.TokenNE:
				return value.NewBoolValue(false), true
			case lex.TokenGT, lex.TokenGE, lex.TokenLT, lex.TokenLE, lex.TokenLike:
				return value.NewBoolValue(false), true
			}
			return nil, false
		}

		// Else if we can only evaluate right
		if !leftOk {
			switch node.Operator.T {
			case lex.TokenIntersects, lex.TokenIN, lex.TokenContains, lex.TokenLike:
				return value.NewBoolValue(false), true
			}
		}

		// Else if we can only evaluate one, we can short circuit as well
		if !leftOk || !rightOk {
			switch node.Operator.T {
			case lex.TokenAnd, lex.TokenLogicAnd:
				return value.NewBoolValue(false), true
			case lex.TokenEqualEqual, lex.TokenEqual:
				return value.NewBoolValue(false), true
			case lex.TokenNE:
				// they are technically not equal?
				return value.NewBoolValue(true), true
			case lex.TokenIN, lex.TokenIntersects:
				return value.NewBoolValue(false), true
			case lex.TokenGT, lex.TokenGE, lex.TokenLT, lex.TokenLE, lex.TokenLike:
				return value.NewBoolValue(false), true
			}
		}

		// Handle different operators
		switch node.Operator.T {
		case lex.TokenEqual, lex.TokenEqualEqual:
			eq, err := value.Equal(left, right)
			if err != nil {
				return value.NewBoolValue(false), true
			}
			return value.NewBoolValue(eq), true

		case lex.TokenNE:
			eq, err := value.Equal(left, right)
			if err != nil {
				return value.NewBoolValue(true), true
			}
			return value.NewBoolValue(!eq), true

		case lex.TokenGT:
			// Handle different value types
			switch lv := left.(type) {
			case value.NumberValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewBoolValue(lv.Val() > rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewBoolValue(lv.Val() > float64(rv.Val())), true
				} else if rv, ok := right.(value.StringValue); ok {
					if rf, err := strconv.ParseFloat(rv.Val(), 64); err == nil {
						return value.NewBoolValue(lv.Val() > rf), true
					}
				}
			case value.IntValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewBoolValue(float64(lv.Val()) > rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewBoolValue(lv.Val() > rv.Val()), true
				}
			case value.StringValue:
				if rv, ok := right.(value.TimeValue); ok {
					leftTime, ok := value.ValueToTime(left)
					if !ok {
						return value.BoolValueFalse, false
					}
					return value.NewBoolValue(rv.Val().Unix() > leftTime.Unix()), true
				}
				if rv, ok := right.(value.StringValue); ok {
					return value.NewBoolValue(lv.Val() > rv.Val()), true
				}
			case value.TimeValue:
				rightTime, ok := value.ValueToTime(right)
				if !ok {
					return value.BoolValueFalse, false
				}
				return value.NewBoolValue(lv.Val().Unix() > rightTime.Unix()), true
			}
			// Try converting to strings
			return value.NewBoolValue(left.ToString() > right.ToString()), true

		case lex.TokenGE:
			// Similar to GT but with >= comparison
			switch lv := left.(type) {
			case value.NumberValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewBoolValue(lv.Val() >= rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewBoolValue(lv.Val() >= float64(rv.Val())), true
				} else if rv, ok := right.(value.StringValue); ok {
					if rf, err := strconv.ParseFloat(rv.Val(), 64); err == nil {
						return value.NewBoolValue(lv.Val() >= rf), true
					}
				}
			case value.IntValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewBoolValue(float64(lv.Val()) >= rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewBoolValue(lv.Val() >= rv.Val()), true
				}
			case value.StringValue:
				// TODO (need this to work for all the operators)
				if rv, ok := right.(value.TimeValue); ok {
					leftTime, ok := value.ValueToTime(left)
					if !ok {
						return value.BoolValueFalse, false
					}
					return value.NewBoolValue(rv.Val().Unix() >= leftTime.Unix()), true
				}
				if rv, ok := right.(value.StringValue); ok {
					return value.NewBoolValue(lv.Val() >= rv.Val()), true
				}
			case value.TimeValue:
				rightTime, ok := value.ValueToTime(right)
				if !ok {
					return value.BoolValueFalse, false
				}
				return value.NewBoolValue(lv.Val().Unix() >= rightTime.Unix()), true
			}
			return value.NewBoolValue(left.ToString() >= right.ToString()), true

		case lex.TokenLT:
			// Similar to GT but with < comparison
			switch lv := left.(type) {
			case value.NumberValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewBoolValue(lv.Val() < rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewBoolValue(lv.Val() < float64(rv.Val())), true
				} else if rv, ok := right.(value.StringValue); ok {
					if rf, err := strconv.ParseFloat(rv.Val(), 64); err == nil {
						return value.NewBoolValue(lv.Val() < rf), true
					}
				}
			case value.IntValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewBoolValue(float64(lv.Val()) < rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewBoolValue(lv.Val() < rv.Val()), true
				}
			case value.StringValue:
				if rv, ok := right.(value.TimeValue); ok {
					leftTime, ok := value.ValueToTime(left)
					if !ok {
						return value.BoolValueFalse, false
					}
					return value.NewBoolValue(rv.Val().Unix() < leftTime.Unix()), true
				}
				if rv, ok := right.(value.StringValue); ok {
					return value.NewBoolValue(lv.Val() < rv.Val()), true
				}
			case value.TimeValue:
				rightTime, ok := value.ValueToTime(right)
				if !ok {
					return value.BoolValueFalse, false
				}
				return value.NewBoolValue(lv.Val().Unix() < rightTime.Unix()), true
			}
			return value.NewBoolValue(left.ToString() < right.ToString()), true

		case lex.TokenLE:
			// Similar to GT but with <= comparison
			switch lv := left.(type) {
			case value.NumberValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewBoolValue(lv.Val() <= rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewBoolValue(lv.Val() <= float64(rv.Val())), true
				} else if rv, ok := right.(value.StringValue); ok {
					if rf, err := strconv.ParseFloat(rv.Val(), 64); err == nil {
						return value.NewBoolValue(lv.Val() <= rf), true
					}
				}
			case value.IntValue:
				//  TODO (Add parsing of strings to ints)
				//  TODO (How to handle string floats?)
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewBoolValue(float64(lv.Val()) <= rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewBoolValue(lv.Val() <= rv.Val()), true
				}
			case value.StringValue:
				if rv, ok := right.(value.TimeValue); ok {
					leftTime, ok := value.ValueToTime(left)
					if !ok {
						return value.BoolValueFalse, false
					}
					return value.NewBoolValue(rv.Val().Unix() <= leftTime.Unix()), true
				}
				if rv, ok := right.(value.StringValue); ok {
					return value.NewBoolValue(lv.Val() <= rv.Val()), true
				}
			case value.TimeValue:
				rightTime, ok := value.ValueToTime(right)
				if !ok {
					return value.BoolValueFalse, false
				}
				return value.NewBoolValue(lv.Val().Unix() <= rightTime.Unix()), true
			}
			return value.NewBoolValue(left.ToString() <= right.ToString()), true

		case lex.TokenPlus:
			// Addition or string concatenation
			switch lv := left.(type) {
			case value.NumberValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewNumberValue(lv.Val() + rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewNumberValue(lv.Val() + float64(rv.Val())), true
				}
			case value.IntValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewNumberValue(float64(lv.Val()) + rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewIntValue(lv.Val() + rv.Val()), true
				}
			}
			// Fallback to string concatenation
			return value.NewStringValue(left.ToString() + right.ToString()), true

		case lex.TokenMinus:
			// Subtraction
			switch lv := left.(type) {
			case value.NumberValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewNumberValue(lv.Val() - rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewNumberValue(lv.Val() - float64(rv.Val())), true
				}
			case value.IntValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewNumberValue(float64(lv.Val()) - rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewIntValue(lv.Val() - rv.Val()), true
				}
			}
			return nil, false

		case lex.TokenMultiply, lex.TokenStar:
			// Multiplication
			switch lv := left.(type) {
			case value.NumberValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewNumberValue(lv.Val() * rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewNumberValue(lv.Val() * float64(rv.Val())), true
				}
			case value.IntValue:
				if rv, ok := right.(value.NumberValue); ok {
					return value.NewNumberValue(float64(lv.Val()) * rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					return value.NewIntValue(lv.Val() * rv.Val()), true
				}
			}
			return nil, false

		case lex.TokenDivide:
			// Division
			switch lv := left.(type) {
			case value.NumberValue:
				if rv, ok := right.(value.NumberValue); ok {
					if rv.Val() == 0 {
						return nil, false // Division by zero
					}
					return value.NewNumberValue(lv.Val() / rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					if rv.Val() == 0 {
						return nil, false // Division by zero
					}
					return value.NewNumberValue(lv.Val() / float64(rv.Val())), true
				}
			case value.IntValue:
				if rv, ok := right.(value.NumberValue); ok {
					if rv.Val() == 0 {
						return nil, false // Division by zero
					}
					return value.NewNumberValue(float64(lv.Val()) / rv.Val()), true
				} else if rv, ok := right.(value.IntValue); ok {
					if rv.Val() == 0 {
						return nil, false // Division by zero
					}
					return value.NewIntValue(lv.Val() / rv.Val()), true
				}
			}
			return nil, false

		case lex.TokenModulus:
			// Modulus (remainder)
			switch lv := left.(type) {
			case value.IntValue:
				if rv, ok := right.(value.IntValue); ok {
					if rv.Val() == 0 {
						return nil, false // Modulus by zero
					}
					return value.NewIntValue(lv.Val() % rv.Val()), true
				}
			case value.NumberValue:
				if rv, ok := right.(value.NumberValue); ok {
					if rv.Val() == 0 {
						return nil, false // Modulus by zero
					}
					return value.NewNumberValue(float64(int64(lv.Val()) % int64(rv.Val()))), true
				} else if rv, ok := right.(value.IntValue); ok {
					if rv.Val() == 0 {
						return nil, false // Modulus by zero
					}
					return value.NewNumberValue(float64(int64(lv.Val()) % rv.Val())), true
				}
			}
			return nil, false

		case lex.TokenLogicAnd, lex.TokenAnd:
			// Logical AND
			leftBool, ok := left.(value.BoolValue)
			if !ok {
				return value.NewBoolValue(false), true
			}

			rightBool, ok := right.(value.BoolValue)
			if !ok {
				return value.NewBoolValue(false), true
			}

			return value.NewBoolValue(leftBool.Val() && rightBool.Val()), true

		case lex.TokenLogicOr, lex.TokenOr:
			// Logical OR
			leftBool, ok := left.(value.BoolValue)
			if !ok {
				return value.NewBoolValue(false), true
			}

			rightBool, ok := right.(value.BoolValue)
			if !ok {
				return value.NewBoolValue(false), true
			}

			return value.NewBoolValue(leftBool.Val() || rightBool.Val()), true

		case lex.TokenContains:
			// Contains operation (string or slice)
			switch lv := left.(type) {
			case value.StringValue:
				if rv, ok := right.(value.StringValue); ok {
					return value.NewBoolValue(strings.Contains(lv.Val(), rv.Val())), true
				}
				return value.NewBoolValue(strings.Contains(lv.Val(), right.ToString())), true

			case value.Slice:
				// Check if right side is in left slice
				for _, item := range lv.SliceValue() {
					if eq, err := value.Equal(item, right); err == nil && eq {
						return value.NewBoolValue(true), true
					}
				}
				return value.NewBoolValue(false), true
			}
			return value.NewBoolValue(false), true

		case lex.TokenLike:
			// LIKE pattern matching
			leftStr, ok := value.ValueToString(left)
			if !ok {
				return value.NewBoolValue(false), true
			}

			rightStr, ok := value.ValueToString(right)
			if !ok {
				return value.NewBoolValue(false), true
			}

			// Convert SQL LIKE pattern to glob pattern
			pattern := strings.Replace(rightStr, "%", "*", -1)

			// Use glob matching (to be imported)
			match, err := glob.Match(pattern, leftStr)
			if err != nil {
				return value.NewBoolValue(false), true
			}

			return value.NewBoolValue(match), true

		case lex.TokenIN, lex.TokenIntersects:
			// IN or INTERSECTS operation
			switch rv := right.(type) {
			case value.Slice:
				if lv, ok := left.(value.Slice); ok {
					// Check if any left item is in right slice
					for _, item := range lv.SliceValue() {
						for _, rightItem := range rv.SliceValue() {
							if eq, err := value.Equal(item, rightItem); err == nil && eq {
								return value.NewBoolValue(true), true
							}
						}
					}
					return value.NewBoolValue(false), true
				}
				if lv, ok := left.(value.Map); ok {
					// Check if any left item is in right slice
					for _, item := range rv.SliceValue() {
						if _, exists := lv.Get(item.ToString()); exists {
							return value.NewBoolValue(exists), true
						}
					}
					return value.NewBoolValue(false), true
				}
				// Check if left side is in right slice
				for _, item := range rv.SliceValue() {
					if eq, err := value.Equal(left, item); err == nil && eq {
						return value.NewBoolValue(true), true
					}
				}
				return value.NewBoolValue(false), true

			case value.Map:
				// Check if left key exists in map
				leftStr, ok := value.ValueToString(left)
				if !ok {
					return value.NewBoolValue(false), true
				}

				_, exists := rv.Get(leftStr)
				return value.NewBoolValue(exists), true
			}
			return value.NewBoolValue(false), true
		}

		return nil, false
	}

	return &CompiledExpr{
		Node:     node,
		EvalFunc: evalFunc,
	}, nil
}

// compileBoolean creates a direct function for boolean operations (AND/OR)
func (c *DirectCompiler) compileBoolean(node *expr.BooleanNode) (*CompiledExpr, error) {
	// Compile all arguments
	argExprs := make([]*CompiledExpr, len(node.Args))
	for i, arg := range node.Args {
		compiled, err := c.compileToFunc(arg)
		if err != nil {
			return nil, err
		}
		argExprs[i] = compiled
	}

	// Create a direct function based on the operator
	evalFunc := func(ctx expr.EvalContext) (value.Value, bool) {
		switch node.Operator.T {
		case lex.TokenLogicAnd, lex.TokenAnd:
			// Short-circuit AND: if any arg is false, return false
			for _, argExpr := range argExprs {
				result, ok := argExpr.EvalFunc(ctx)
				if !ok {
					// If we can't evaluate an argument, for AND we return false
					if node.Negated() {
						return value.NewBoolValue(true), true
					}
					return value.NewBoolValue(false), true
				}

				if boolVal, ok := result.(value.BoolValue); ok {
					if !boolVal.Val() {
						// Short-circuit: found a false value
						if node.Negated() {
							return value.NewBoolValue(true), true
						}
						return value.NewBoolValue(false), true
					}
				} else {
					// Non-boolean value in AND, treat as false
					if node.Negated() {
						return value.NewBoolValue(true), true
					}
					return value.NewBoolValue(false), true
				}
			}

			// All arguments were true
			if node.Negated() {
				return value.NewBoolValue(false), true
			}
			return value.NewBoolValue(true), true

		case lex.TokenLogicOr, lex.TokenOr:
			// Short-circuit OR: if any arg is true, return true
			for _, argExpr := range argExprs {
				result, ok := argExpr.EvalFunc(ctx)
				if !ok {
					continue // Try the next argument
				}

				if boolVal, ok := result.(value.BoolValue); ok {
					if boolVal.Val() {
						// Short-circuit: found a true value
						if node.Negated() {
							return value.NewBoolValue(false), true
						}
						return value.NewBoolValue(true), true
					}
				}
			}

			// No argument was true
			if node.Negated() {
				return value.NewBoolValue(true), true
			}
			return value.NewBoolValue(false), true
		}

		// Unsupported operator
		return nil, false
	}

	return &CompiledExpr{
		Node:     node,
		EvalFunc: evalFunc,
	}, nil
}

// compileUnary creates a direct function for unary operations
func (c *DirectCompiler) compileUnary(node *expr.UnaryNode) (*CompiledExpr, error) {
	// Compile the argument
	argExpr, err := c.compileToFunc(node.Arg)
	if err != nil {
		return nil, err
	}

	// Create a direct function based on the operator
	evalFunc := func(ctx expr.EvalContext) (value.Value, bool) {
		switch node.Operator.T {
		case lex.TokenNegate:
			result, ok := argExpr.EvalFunc(ctx)
			if !ok {
				return value.NewBoolValue(false), true
			}

			if boolVal, ok := result.(value.BoolValue); ok {
				return value.NewBoolValue(!boolVal.Val()), true
			}

			return value.NewBoolValue(false), true

		case lex.TokenMinus:
			result, ok := argExpr.EvalFunc(ctx)
			if !ok {
				return nil, false
			}

			if numVal, ok := result.(value.NumberValue); ok {
				return value.NewNumberValue(-numVal.Val()), true
			} else if intVal, ok := result.(value.IntValue); ok {
				return value.NewIntValue(-intVal.Val()), true
			}

			return nil, false

		case lex.TokenExists:
			result, ok := argExpr.EvalFunc(ctx)
			if !ok {
				return value.NewBoolValue(false), true
			}

			if result == nil || result.Nil() {
				return value.NewBoolValue(false), true
			}

			return value.NewBoolValue(true), true
		}

		return nil, false
	}

	return &CompiledExpr{
		Node:     node,
		EvalFunc: evalFunc,
	}, nil
}

// compileIdentity creates a direct function for identity (variable) references
func (c *DirectCompiler) compileIdentity(node *expr.IdentityNode) (*CompiledExpr, error) {
	// Boolean identities (true/false)
	if node.IsBooleanIdentity() {
		boolValue := node.Bool()
		return &CompiledExpr{
			Node: node,
			EvalFunc: func(ctx expr.EvalContext) (value.Value, bool) {
				return value.NewBoolValue(boolValue), true
			},
		}, nil
	}

	// Create a direct lookup function
	evalFunc := func(ctx expr.EvalContext) (value.Value, bool) {
		if node.HasLeftRight() {
			return ctx.Get(node.OriginalText())
		}
		return ctx.Get(node.Text)
	}

	return &CompiledExpr{
		Node:     node,
		EvalFunc: evalFunc,
	}, nil
}

// compileNumber creates a direct function for numeric literals
func (c *DirectCompiler) compileNumber(node *expr.NumberNode) (*CompiledExpr, error) {
	if node.IsInt {
		intValue := node.Int64
		return &CompiledExpr{
			Node: node,
			EvalFunc: func(ctx expr.EvalContext) (value.Value, bool) {
				return value.NewIntValue(intValue), true
			},
		}, nil
	}

	floatValue := node.Float64
	return &CompiledExpr{
		Node: node,
		EvalFunc: func(ctx expr.EvalContext) (value.Value, bool) {
			return value.NewNumberValue(floatValue), true
		},
	}, nil
}

// compileString creates a direct function for string literals
func (c *DirectCompiler) compileString(node *expr.StringNode) (*CompiledExpr, error) {
	strValue := node.Text
	return &CompiledExpr{
		Node: node,
		EvalFunc: func(ctx expr.EvalContext) (value.Value, bool) {
			return value.NewStringValue(strValue), true
		},
	}, nil
}

// compileFunc creates a direct function for function calls
func (c *DirectCompiler) compileFunc(node *expr.FuncNode) (*CompiledExpr, error) {
	// Compile all arguments
	argExprs := make([]*CompiledExpr, len(node.Args))
	for i, arg := range node.Args {
		compiled, err := c.compileToFunc(arg)
		if err != nil {
			return nil, err
		}
		argExprs[i] = compiled
	}

	// Get the function implementation
	// funcName := strings.ToLower(node.Name)

	// Create a direct function
	evalFunc := func(ctx expr.EvalContext) (value.Value, bool) {
		// Evaluate all arguments
		args := make([]value.Value, len(argExprs))
		for i, argExpr := range argExprs {
			arg, ok := argExpr.EvalFunc(ctx)
			if !ok {
				arg = value.NewNilValue()
			}
			args[i] = arg
		}

		// Execute function
		return node.Eval(ctx, args)
	}

	return &CompiledExpr{
		Node:     node,
		EvalFunc: evalFunc,
	}, nil
}

// compileTernary creates a direct function for ternary operations (BETWEEN)
func (c *DirectCompiler) compileTernary(node *expr.TriNode) (*CompiledExpr, error) {
	// Only BETWEEN is currently supported
	if node.Operator.T != lex.TokenBetween {
		return nil, fmt.Errorf("unsupported ternary operator: %v", node.Operator.T)
	}

	// Compile all arguments
	valueExpr, err := c.compileToFunc(node.Args[0])
	if err != nil {
		return nil, err
	}

	lowerExpr, err := c.compileToFunc(node.Args[1])
	if err != nil {
		return nil, err
	}

	upperExpr, err := c.compileToFunc(node.Args[2])
	if err != nil {
		return nil, err
	}

	// Create a direct function
	evalFunc := func(ctx expr.EvalContext) (value.Value, bool) {
		// Evaluate all arguments
		val, valueOk := valueExpr.EvalFunc(ctx)
		if !valueOk {
			return nil, false
		}

		lower, lowerOk := lowerExpr.EvalFunc(ctx)
		if !lowerOk {
			return nil, false
		}

		upper, upperOk := upperExpr.EvalFunc(ctx)
		if !upperOk {
			return nil, false
		}

		// Compare based on value types
		switch v := val.(type) {
		case value.NumberValue:
			lowerVal, lowerOk := value.ValueToFloat64(lower)
			upperVal, upperOk := value.ValueToFloat64(upper)

			if lowerOk && upperOk {
				result := v.Val() > lowerVal && v.Val() < upperVal
				if node.Negated() {
					return value.NewBoolValue(!result), true
				}
				return value.NewBoolValue(result), true
			}

		case value.IntValue:
			lowerVal, lowerOk := value.ValueToInt64(lower)
			upperVal, upperOk := value.ValueToInt64(upper)

			if lowerOk && upperOk {
				result := v.Val() > lowerVal && v.Val() < upperVal
				if node.Negated() {
					return value.NewBoolValue(!result), true
				}
				return value.NewBoolValue(result), true
			}

		case value.TimeValue:
			lowerTime, lowerOk := value.ValueToTime(lower)
			upperTime, upperOk := value.ValueToTime(upper)

			if lowerOk && upperOk {
				result := v.Val().After(lowerTime) && v.Val().Before(upperTime)
				if node.Negated() {
					return value.NewBoolValue(!result), true
				}
				return value.NewBoolValue(result), true
			}
		}

		// Fallback to string comparison
		valueStr := val.ToString()
		lowerStr := lower.ToString()
		upperStr := upper.ToString()

		result := valueStr > lowerStr && valueStr < upperStr
		if node.Negated() {
			return value.NewBoolValue(!result), true
		}
		return value.NewBoolValue(result), true
	}

	return &CompiledExpr{
		Node:     node,
		EvalFunc: evalFunc,
	}, nil
}

// compileArray creates a direct function for array nodes
func (c *DirectCompiler) compileArray(node *expr.ArrayNode) (*CompiledExpr, error) {
	// Compile all elements
	elemExprs := make([]*CompiledExpr, len(node.Args))
	for i, arg := range node.Args {
		compiled, err := c.compileToFunc(arg)
		if err != nil {
			return nil, err
		}
		elemExprs[i] = compiled
	}

	// Create a direct function
	evalFunc := func(ctx expr.EvalContext) (value.Value, bool) {
		// Evaluate all elements
		elems := make([]value.Value, len(elemExprs))
		for i, elemExpr := range elemExprs {
			elem, ok := elemExpr.EvalFunc(ctx)
			if !ok {
				elem = value.NewNilValue()
			}
			elems[i] = elem
		}

		// Create slice value
		return value.NewSliceValues(elems), true
	}

	return &CompiledExpr{
		Node:     node,
		EvalFunc: evalFunc,
	}, nil
}

// compileInclude creates a direct function for include nodes
func (c *DirectCompiler) compileInclude(node *expr.IncludeNode) (*CompiledExpr, error) {
	includeID := node.Identity.Text

	// Create a direct function
	evalFunc := func(ctx expr.EvalContext) (value.Value, bool) {
		// Check for IncludeCache
		if cacheCtx, hasCacheCtx := ctx.(expr.IncludeCacheContextV2); hasCacheCtx {
			matches, err := cacheCtx.GetOrSet(includeID, func() (bool, error) {
				return evaluateInclude(ctx, node)
			})

			if err != nil {
				if node.Negated() {
					return value.NewBoolValue(true), true
				}
				return nil, false
			}

			if node.Negated() {
				return value.NewBoolValue(!matches), true
			}
			return value.NewBoolValue(matches), true
		}

		// Non-cached evaluation
		matches, err := evaluateInclude(ctx, node)
		if err != nil {
			if node.Negated() {
				return value.NewBoolValue(true), true
			}
			return nil, false
		}

		if node.Negated() {
			return value.NewBoolValue(!matches), true
		}
		return value.NewBoolValue(matches), true
	}

	return &CompiledExpr{
		Node:     node,
		EvalFunc: evalFunc,
	}, nil
}

// evaluateInclude evaluates an include node
func evaluateInclude(ctx expr.EvalContext, node *expr.IncludeNode) (bool, error) {
	includer, ok := ctx.(expr.EvalIncludeContext)
	if !ok {
		return false, fmt.Errorf("no inclusion context")
	}

	// Resolve the included expression
	includedExpr := node.ExprNode
	if includedExpr == nil {
		var err error
		includedExpr, err = includer.Include(node.Identity.Text)
		if err != nil || includedExpr == nil {
			return false, fmt.Errorf("walking include: %w", err)
		}

		// Save for future use
		node.ExprNode = includedExpr
	}

	// Check for wildcard includes
	if idNode, ok := includedExpr.(*expr.IdentityNode); ok {
		if idNode.Text == "*" || idNode.Text == "match_all" {
			return true, nil
		}
	}

	// Evaluate the included expression
	result, ok := vm.Eval(ctx, includedExpr)
	if !ok {
		return false, fmt.Errorf("evaluating expression")
	}

	// Convert to boolean
	if boolVal, ok := result.(value.BoolValue); ok {
		return boolVal.Val(), nil
	}

	return false, nil
}

// compileValue creates a direct function for value nodes
func (c *DirectCompiler) compileValue(node *expr.ValueNode) (*CompiledExpr, error) {
	if node.Value == nil {
		return &CompiledExpr{
			Node: node,
			EvalFunc: func(ctx expr.EvalContext) (value.Value, bool) {
				return nil, false
			},
		}, nil
	}

	// Store value for direct return
	val := node.Value

	return &CompiledExpr{
		Node: node,
		EvalFunc: func(ctx expr.EvalContext) (value.Value, bool) {
			return val, true
		},
	}, nil
}

// hashNode creates a hash of an expression node
func hashNode(node expr.Node) uint64 {
	h := fnv.New64()
	h.Write([]byte(node.String()))
	return h.Sum64()
}

func hashFilter(filter *rel.FilterStatement) uint64 {
	h := fnv.New64()
	h.Write([]byte(filter.String()))
	return h.Sum64()
}
