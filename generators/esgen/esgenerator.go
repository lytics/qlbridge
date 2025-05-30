package esgen

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

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
	f, err := fg.walkExpr(node, 0)
	if err != nil {
		return nil, err
	}
	payload.Filter = f

	//TODO order by -> sort
	return payload, nil
}

// expr dispatches to node-type-specific methods
func (fg *FilterGenerator) walkExpr(node expr.Node, depth int) (any, error) {
	if depth > MaxDepth {
		return nil, errors.New("hit max depth on segment generation. bad query?")
	}
	var err error
	var filter any
	switch n := node.(type) {
	case *expr.UnaryNode:
		// Urnaries do their own negation
		filter, err = fg.unaryExpr(n, depth+1)
	case *expr.BooleanNode:
		// Also do their own negation
		filter, err = fg.booleanExpr(n, depth+1)
	case *expr.BinaryNode:
		filter, err = fg.binaryExpr(n, depth+1)
	case *expr.TriNode:
		filter, err = fg.triExpr(n, depth+1)
	case *expr.IdentityNode:
		iv := strings.ToLower(n.Text)
		switch iv {
		case "match_all", "*":
			return MatchAll, nil
		}
		//HACK As a special case support true as "match_all"; we could support
		//    false -> MatchNone, but that seems useless and wasteful of ES cpu.
		if n.Bool() {
			return MatchAll, nil
		}
		return nil, fmt.Errorf("unsupported identity node in expression: %s expr: %s", node.NodeType(), node)
	case *expr.IncludeNode:
		if incErr := vm.ResolveIncludes(fg.inc, n); incErr != nil {
			return nil, incErr
		}
		filter, err = fg.walkExpr(n.ExprNode, depth+1)
	case *expr.FuncNode:
		filter, err = fg.funcExpr(n, depth+1)
	case *expr.StringNode:
		// Special case for *.
		iv := strings.ToLower(n.Text)
		switch iv {
		case "match_all", "*":
			return MatchAll, nil
		}
		return nil, fmt.Errorf("unsupported string node in expression: %s expr: %s", node.NodeType(), node)
	default:
		return nil, fmt.Errorf("unsupported node in expression: %s expr: %s", node.NodeType(), node)
	}
	if err != nil {
		// Convert MissingField errors to a logical `false`
		var errMissingField *gentypes.ErrorMissingField
		if errors.As(err, &errMissingField) {
			return MatchNone, nil
		}
		return nil, err
	}

	nn, isNegateable := node.(expr.NegateableNode)
	if isNegateable {
		if nn.Negated() {
			return NotFilter(filter), nil
		}
	}
	return filter, nil
}

func (fg *FilterGenerator) unaryExpr(node *expr.UnaryNode, depth int) (any, error) {
	switch node.Operator.T {
	case lex.TokenExists:
		ft, err := fg.fieldType(node.Arg)
		if err != nil {
			return nil, err
		}
		return Exists(ft), nil

	case lex.TokenNegate:
		inner, err := fg.walkExpr(node.Arg, depth+1)
		if err != nil {
			return nil, err
		}
		return NotFilter(inner), nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", node.Operator.T)
	}
}

// filters returns a boolean expression
func (fg *FilterGenerator) booleanExpr(bn *expr.BooleanNode, depth int) (any, error) {
	if depth > MaxDepth {
		return nil, errors.New("hit max depth on segment generation. bad query?")
	}
	and := true
	switch bn.Operator.T {
	case lex.TokenAnd, lex.TokenLogicAnd:
	case lex.TokenOr, lex.TokenLogicOr:
		and = false
	default:
		return nil, fmt.Errorf("unexpected op %v", bn.Operator)
	}

	items := make([]any, 0, len(bn.Args))
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
				return MatchNone, nil
			}
			return nil, err
		}
		items = append(items, it)
	}

	if len(items) == 1 {
		// Be nice and omit the useless boolean filter since there's only 1 item
		return items[0], nil
	}

	var bf *BoolFilter
	if and {
		bf = AndFilter(items)
	} else {
		bf = OrFilter(items)
	}

	return bf, nil
}

func (fg *FilterGenerator) binaryExpr(node *expr.BinaryNode, _ int) (any, error) {
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
		if lhs.Nested() {
			fieldName, _ := lhs.PrefixAndValue(rhs)
			return Nested(lhs, Term(fieldName, rhs)), nil
			//return nil, fmt.Errorf("qlindex: == not supported for nested types %q", lhs.String())
		}
		return Term(lhs.Field, rhs), nil

	case lex.TokenNE: // ident(0) != literal(1)
		rhs, ok := scalar(node.Args[1])
		if !ok {
			return nil, fmt.Errorf("unsupported second argument for equality: %s expr: %s", node.Args[1].NodeType(), node.Args[1])
		}
		if lhs.Nested() {
			fieldName, _ := lhs.PrefixAndValue(rhs)
			return NotFilter(Nested(lhs, Term(fieldName, rhs))), nil
		}
		return NotFilter(Term(lhs.Field, rhs)), nil

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
		args := make([]any, 0, len(array.Args))
		for _, nodearg := range array.Args {
			strarg, ok := scalar(nodearg)
			if !ok {
				return nil, fmt.Errorf("non-scalar argument in %s clause: %s expr: %s", op, nodearg.NodeType(), nodearg)
			}
			args = append(args, strarg)
		}

		return In(lhs, args), nil

	default:
		return nil, fmt.Errorf("unsupported binary expression: %s", op)
	}
}

func (fg *FilterGenerator) triExpr(node *expr.TriNode, _ int) (any, error) {
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

func (fg *FilterGenerator) funcExpr(node *expr.FuncNode, _ int) (any, error) {
	switch node.Name {
	case "timewindow":
		// see entity.EvalTimeWindow for code implementation. Checks if the contextual time is within the time buckets provided
		// by the parameters
		if len(node.Args) != 3 {
			return nil, fmt.Errorf("'timewindow' function requires 3 arguments, got %d", len(node.Args))
		}
		//  We are applying the function to the named field, but the caller *can't* just use the fieldname (which would
		// evaluate to nothing, as the field isn't

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
	case "geodistance":
		if len(node.Args) != 3 {
			return nil, fmt.Errorf("'geodistance' function requires 3 arguments, got %d", len(node.Args))
		}
		lhs, err := fg.fieldType(node.Args[0])
		if err != nil {
			return nil, err
		}
		n, ok := node.Args[1].(*expr.StringNode)
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported type for 'geodistance' location argument. must be string, got %s", node.Args[1].NodeType())
		}
		lat, lon, ok := gentypes.StringToLatLng(n.Text)
		if !ok {
			return nil, fmt.Errorf("qlindex: unsupported format for 'geodistance' location. must be \"latitude,longitude\", got %s", n.Text)
		}
		var distance float64
		switch n := node.Args[2].(type) {
		case *expr.NumberNode:
			if n.IsFloat {
				distance = n.Float64
			} else {
				distance = float64(n.Int64)
			}
		case *expr.StringNode:
			f, err := strconv.ParseFloat(n.Text, 64)
			if err != nil {
				return nil, fmt.Errorf("qlindex: unsupported format for 'geodistance' distance. must be number, got %s", n.Text)
			}
			distance = f
		default:
			return nil, fmt.Errorf("qlindex: unsupported type for 'geodistance' distance argument. must be number, got %s", node.Args[2].NodeType())
		}
		return makeGeoDistanceQuery(lhs, lat, lon, distance), nil
	}
	return nil, fmt.Errorf("qlindex: unsupported function: %s", node.Name)
}

func makeGeoDistanceQuery(lhs *gentypes.FieldType, lat, lon, distance float64) any {
	return &GeoDistanceFilter{
		GeoDistance: map[string]any{
			"distance": fmt.Sprintf("%fkm", distance),
			fmt.Sprintf("%s.location", lhs.Field): map[string]any{
				"lat": lat,
				"lon": lon,
			},
			"distance_type": "plane",
		},
	}
}
