package blevegen

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2/search/query"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/generators/gentypes"
	"github.com/lytics/qlbridge/lex"
	"github.com/lytics/qlbridge/vm"
)

var (
	// MaxDepth specifies the depth at which we are certain the filter generator is in an endless loop
	// This *shouldn't* happen, but is better than a stack overflow
	MaxDepth = 1000
)

// copy-pasta from entity to avoid the import
// when we actually parameterize this we will need to do it differently anyway
func DayBucket(dt time.Time) int {
	return int(dt.UnixNano() / int64(24*time.Hour))
}

type FilterGenerator struct {
	ts     time.Time
	inc    expr.Includer
	schema gentypes.SchemaColumns
}

func NewGenerator(ts time.Time, inc expr.Includer, s gentypes.SchemaColumns) *FilterGenerator {
	return &FilterGenerator{ts: ts, inc: inc, schema: s}
}

func (fg *FilterGenerator) fieldType(n expr.Node) (*gentypes.FieldType, error) {
	return fieldType(fg.schema, n)
}

func (fg *FilterGenerator) WalkExpr(node expr.Node) (*gentypes.Payload, error) {
	payload := &gentypes.Payload{Size: new(int)}
	q, err := fg.walkExpr(node, 0)
	if err != nil {
		return nil, err
	}
	payload.Filter = q

	// Bleve Query wrapped in a payload
	// TODO: Handle sort, size, etc.
	return payload, nil
}

// walkExpr dispatches to node-type-specific methods
func (fg *FilterGenerator) walkExpr(node expr.Node, depth int) (query.Query, error) {
	if depth > MaxDepth {
		return nil, errors.New("hit max depth on segment generation. bad query?")
	}

	var err error
	var q query.Query
	switch n := node.(type) {
	case *expr.UnaryNode:
		// Unaries do their own negation
		q, err = fg.unaryExpr(n, depth+1)
	case *expr.BooleanNode:
		// Also do their own negation
		q, err = fg.booleanExpr(n, depth+1)
	case *expr.BinaryNode:
		q, err = fg.binaryExpr(n, depth+1)
	case *expr.TriNode:
		q, err = fg.triExpr(n, depth+1)
	case *expr.IdentityNode:
		iv := strings.ToLower(n.Text)
		switch iv {
		case "match_all", "*":
			return query.NewMatchAllQuery(), nil
		}
		// As a special case support true as "match_all"
		if n.Bool() {
			return query.NewMatchAllQuery(), nil
		}
		return nil, fmt.Errorf("unsupported dangling identity node in expression: %s expr: %s", node.NodeType(), node)
	case *expr.IncludeNode:
		if incErr := vm.ResolveIncludes(fg.inc, n); incErr != nil {
			return nil, incErr
		}
		q, err = fg.walkExpr(n.ExprNode, depth+1)
	case *expr.FuncNode:
		q, err = fg.funcExpr(n, depth+1)
	case *expr.StringNode:
		// Special case for *.
		iv := strings.ToLower(n.Text)
		switch iv {
		case "match_all", "*":
			return query.NewMatchAllQuery(), nil
		}
		return nil, fmt.Errorf("unsupported string node in expression: %s expr: %s", node.NodeType(), node)
	default:
		return nil, fmt.Errorf("unsupported node in expression: %s expr: %s", node.NodeType(), node)
	}
	if err != nil {
		// Convert MissingField errors to a logical `false`
		var errMissingField *gentypes.ErrorMissingField
		if errors.As(err, &errMissingField) {
			return query.NewMatchNoneQuery(), nil
		}
		return nil, err
	}

	nn, isNegateable := node.(expr.NegateableNode)
	if isNegateable && nn.Negated() {
		// Create a boolean query with this as a "must not"
		boolQuery := query.NewBooleanQuery(nil, nil, nil)
		boolQuery.AddMustNot(q)
		return boolQuery, nil
	}
	return q, nil
}

func (fg *FilterGenerator) unaryExpr(node *expr.UnaryNode, depth int) (query.Query, error) {
	switch node.Operator.T {
	case lex.TokenExists:
		ft, err := fg.fieldType(node.Arg)
		if err != nil {
			return nil, err
		}

		// Create a DocValueQuery for the existence check
		// Bleve doesn't have a direct exists query, so we use a WildcardQuery with "*"
		// which will match any value in the field
		fieldName := ft.Field

		wildcardQuery := query.NewWildcardQuery("*")
		wildcardQuery.SetField(fieldName)
		// wildcardQuery := query.NewMatchAllQuery()
		return wildcardQuery, nil

	case lex.TokenNegate:
		inner, err := fg.walkExpr(node.Arg, depth+1)
		if err != nil {
			return nil, err
		}

		// Create a boolean query with the inner query as a "must not"
		boolQuery := query.NewBooleanQuery(nil, nil, nil)
		boolQuery.AddMustNot(inner)
		return boolQuery, nil

	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", node.Operator.T)
	}
}

// booleanExpr returns a boolean expression
func (fg *FilterGenerator) booleanExpr(bn *expr.BooleanNode, depth int) (query.Query, error) {
	if depth > MaxDepth {
		return nil, errors.New("hit max depth on segment generation. bad query?")
	}

	// Create a boolean query
	boolQuery := query.NewBooleanQuery(nil, nil, nil)

	and := true
	switch bn.Operator.T {
	case lex.TokenAnd, lex.TokenLogicAnd:
		// Default is AND
	case lex.TokenOr, lex.TokenLogicOr:
		and = false
	default:
		return nil, fmt.Errorf("unexpected op %v", bn.Operator)
	}

	for _, fe := range bn.Args {
		it, err := fg.walkExpr(fe, depth+1)
		if err != nil {
			// Convert MissingField errors to a logical `false`
			var errMissingField *gentypes.ErrorMissingField
			if errors.As(err, &errMissingField) {
				if !and {
					// Simply skip missing fields in ORs
					continue
				}
				// Convert ANDs to false
				return query.NewMatchNoneQuery(), nil
			}
			return nil, err
		}

		// Add the query to the boolean query
		if and {
			boolQuery.AddMust(it)
		} else {
			boolQuery.AddShould(it)
		}
	}

	// If this is an OR query, we need at least one should match
	if !and && boolQuery.Should != nil {
		boolQuery.SetMinShould(1)
	}

	return boolQuery, nil
}

func convertToFloat64(value interface{}) float64 {
	switch v := value.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	default:
		return 0.0 // Default value if the type is not handled
	}
}

func queryForScalarArg(fieldName string, rhs any) query.Query {
	// Handle different types of equality queries
	switch v := rhs.(type) {
	case string:
		q := query.NewMatchQuery("^" + v + "$")
		q.SetField(fieldName)
		return q
	case int, int64, float64:
		// For numeric types
		term := convertToFloat64(v)
		t := true
		termQuery := query.NewNumericRangeInclusiveQuery(&term, &term, &t, &t)
		termQuery.SetField(fieldName)
		return termQuery

	case bool:
		// For boolean types
		termQuery := query.NewBoolFieldQuery(v)
		termQuery.SetField(fieldName)
		return termQuery

	case time.Time:
		// For time types
		dateQuery := query.NewDateRangeQuery(v, v)
		dateQuery.SetField(fieldName)
		t := true
		dateQuery.InclusiveStart = &t
		dateQuery.InclusiveEnd = &t
		return dateQuery

	default:
		// Default for other types
		termStr := fmt.Sprintf("%v", v)
		termQuery := query.NewTermQuery(termStr)
		termQuery.SetField(fieldName)
		return termQuery
	}
}

func (fg *FilterGenerator) binaryExpr(node *expr.BinaryNode, _ int) (query.Query, error) {
	// Type check binary expression arguments as they must be:
	// Identifier-Operator-Literal
	lhs, err := fg.fieldType(node.Args[0])
	if err != nil {
		return nil, err
	}

	switch op := node.Operator.T; op {
	case lex.TokenGE, lex.TokenLE, lex.TokenGT, lex.TokenLT:
		return makeRange(lhs, op, node.Args[1])

	case lex.TokenEqual, lex.TokenEqualEqual: // the VM supports both = and ==
		rhs, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("unsupported second argument for equality: %s expr: %s", node.Args[1].NodeType(), node.Args[1])
		}

		return queryForScalarArg(lhs.Field, rhs), nil
	case lex.TokenNE: // ident(0) != literal(1)
		rhs, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("unsupported second argument for inequality: %s expr: %s", node.Args[1].NodeType(), node.Args[1])
		}

		q := queryForScalarArg(lhs.Field, rhs)
		// Negate the term query
		boolQuery := query.NewBooleanQuery(nil, nil, nil)
		boolQuery.AddMustNot(q)
		return boolQuery, nil
	case lex.TokenContains: // ident CONTAINS literal
		rhsstr := ""
		switch rhst := node.Args[1].(type) {
		case *expr.StringNode:
			rhsstr = rhst.Text
		case *expr.IdentityNode:
			rhsstr = rhst.Text
		case *expr.NumberNode:
			rhsstr = rhst.Text
		default:
			return nil, fmt.Errorf("unsupported non-string argument for CONTAINS: %s expr: %v", node.Args[1].NodeType(), node.Args[1])
		}
		return makeWildcard(lhs, rhsstr, true)

	case lex.TokenLike: // ident LIKE literal
		rhsstr := ""
		switch rhst := node.Args[1].(type) {
		case *expr.StringNode:
			rhsstr = rhst.Text
		case *expr.IdentityNode:
			rhsstr = rhst.Text
		case *expr.NumberNode:
			rhsstr = rhst.Text
		default:
			return nil, fmt.Errorf("unsupported non-string argument for LIKE pattern: %s expr: %v", node.Args[1].NodeType(), node.Args[1])
		}
		return makeWildcard(lhs, rhsstr, false)

	case lex.TokenIN, lex.TokenIntersects:
		// Build up list of arguments
		array, ok := node.Args[1].(*expr.ArrayNode)
		if !ok {
			return nil, fmt.Errorf("second argument to node must be an array, found: %v expr: %v", node.Args[1].NodeType(), node.Args[1])
		}

		// For Bleve, we use a disjunction query (OR)
		disjQuery := query.NewDisjunctionQuery(nil)

		for _, nodearg := range array.Args {
			strarg, ok := scalar(nodearg)
			if !ok {
				return nil, fmt.Errorf("non-scalar argument in %s clause: %s expr: %s", op, nodearg.NodeType(), nodearg)
			}
			q := queryForScalarArg(lhs.Field, strarg)
			disjQuery.AddQuery(q)
		}

		return disjQuery, nil

	default:
		return nil, fmt.Errorf("unsupported binary expression: %s", op)
	}
}

func (fg *FilterGenerator) triExpr(node *expr.TriNode, _ int) (query.Query, error) {
	switch op := node.Operator.T; op {
	case lex.TokenBetween: // a BETWEEN b AND c
		// Type check ternary expression arguments as they must be:
		// Identifier(0) BETWEEN Literal(1) AND Literal(2)
		lhs, err := fg.fieldType(node.Args[0])
		if err != nil {
			return nil, err
		}
		lower, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("unsupported type for first argument of BETWEEN expression: %s expr: %v", node.Args[1].NodeType(), node.Args[1])
		}
		upper, ok := scalar(node.Args[2])
		if !ok {
			return nil, fmt.Errorf("unsupported type for second argument of BETWEEN expression: %s expr: %v", node.Args[1].NodeType(), node.Args[1])
		}
		return makeBetween(lhs, lower, upper)
	}
	return nil, fmt.Errorf("unsupported ternary expression: %s", node.Operator.T)
}

func (fg *FilterGenerator) funcExpr(node *expr.FuncNode, _ int) (query.Query, error) {
	switch node.Name {
	case "timewindow":
		// See entity.EvalTimeWindow for code implementation. Checks if the contextual time is within the time buckets provided
		// by the parameters
		if len(node.Args) != 3 {
			return nil, fmt.Errorf("'timewindow' function requires 3 arguments, got %d", len(node.Args))
		}

		lhs, err := fg.fieldType(node.Args[0])
		if err != nil {
			return nil, err
		}

		threshold, ok := node.Args[1].(*expr.NumberNode)
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported type for 'timewindow' argument. must be number, got %s arg: %v", node.Args[1].NodeType(), node.Args[1])
		}

		if !threshold.IsInt {
			return nil, fmt.Errorf("qlindex: unsupported type for 'timewindow' argument. must be number, got %s arg: %v", node.Args[1].NodeType(), node.Args[1])
		}

		window, ok := node.Args[2].(*expr.NumberNode)
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported type for 'timewindow' argument. must be number, got %s expr: %v", node.Args[2].NodeType(), node.Args[2])
		}

		if !window.IsInt {
			return nil, fmt.Errorf("qlindex: unsupported type for 'timewindow' argument. must be integer, got float %s expr: %v", node.Args[2].NodeType(), node.Args[2])
		}

		return makeTimeWindowQuery(lhs, threshold.Int64, window.Int64, int64(DayBucket(fg.ts)))
	}
	return nil, fmt.Errorf("qlindex: unsupported function: %s", node.Name)
}
