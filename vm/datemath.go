package vm

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/lytics/datemath"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/lex"
	"github.com/lytics/qlbridge/value"
)

// DateConverter can help inspect a boolean expression to determine if there is
// date-math in it.  If there is datemath, can calculate the time boundary
// where the expression may possibly change from true to false.
// - Must be boolean expression
// - Only calculates the first boundary
// - Only calculates POSSIBLE boundary, given complex logic (ors etc) may NOT change.
type DateConverter struct {
	HasDateMath bool      // Does this have date math in it all?
	bt          time.Time // The possible boundary time when expression flips true/false
	at          time.Time // The Time to use as "now" or reference point
	err         error
}

func FindBoundary(anchorTime time.Time, ctx expr.EvalIncludeContext, fns BoundaryFns) (time.Time, error) {
	dc := &DateConverter{
		at: anchorTime,
	}
	for _, fn := range fns {
		fn(dc, ctx)
	}
	return dc.bt, dc.err
}

type BoundaryFns []func(*DateConverter, expr.EvalIncludeContext)

func CalcBoundaryFns(n expr.Node) BoundaryFns {
	return findDateMathFn(n)
}

// NewDateConverter takes a node expression
func NewDateConverter(ctx expr.EvalIncludeContext, n expr.Node) (*DateConverter, error) {
	dc := &DateConverter{
		at: time.Now(),
	}
	fns := findDateMathFn(n)
	dc.bt, dc.err = FindBoundary(dc.at, ctx, fns)
	if dc.err == nil && len(fns) > 0 {
		dc.HasDateMath = true
	}
	return dc, dc.err
}
func compareBoundaries(currBoundary, newBoundary time.Time) time.Time {
	// Should we check for is zero on the newBoundary?
	if currBoundary.IsZero() || newBoundary.Before(currBoundary) {
		return newBoundary
	}
	return currBoundary
}
func evalBoundary(anchorTime, currBoundary time.Time, lhv value.Value, op lex.TokenType, val string) (time.Time, error) {
	// Given Anchor Time At calculate Relative Time Rt
	rt, err := datemath.EvalAnchor(anchorTime, val)
	if err != nil {
		return currBoundary, err
	}

	ct, err := value.ValueToTime(lhv)
	if err != nil && strings.Contains(err.Error(), "slice values") {
		switch op {
		case lex.TokenGT, lex.TokenGE:
			bt := currBoundary
			for _, val := range lhv.(value.SliceValue).SliceValue() {
				ct, err := value.ValueToTime(val)
				if err != nil {
					return time.Time{}, fmt.Errorf("converting slice value: %w", err)
				}
				if rt.Before(ct) {
					bt = compareBoundaries(currBoundary, anchorTime.Add(ct.Sub(rt)))
				}
			}
			return bt, nil
		case lex.TokenLT, lex.TokenLE:
			bt := currBoundary
			for _, val := range lhv.(value.SliceValue).SliceValue() {
				ct, err := value.ValueToTime(val)
				if err != nil {
					return time.Time{}, fmt.Errorf("converting slice value: %w", err)
				}
				if !ct.Before(rt) {
					bt = compareBoundaries(bt, anchorTime.Add(ct.Sub(rt)))
				}
			}
			return bt, nil
		default:
			return currBoundary, nil
		}
	} else if err != nil {
		return currBoundary, fmt.Errorf("Could not convert %T: %v to time.Time %w", lhv, lhv, err)
	}

	// Ct = Comparison time, left hand side of expression
	// At = Anchor Time
	// Rt = Relative time result of Anchor Time offset by datemath "now-3d"
	// Bt = Boundary time = calculated time at which expression will change boolean expression value
	switch op {
	case lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE:
		// none of these are supported operators for finding boundary
		return currBoundary, nil
	case lex.TokenGT, lex.TokenGE:
		// 1) ----------- Ct --------------     Rt < Ct
		//        Rt                            Ct > "now+-1d" = true but will be true when at + (ct - rt)
		//         ------Bt
		//
		// 2) ------------- Ct ------------     Ct < Rt
		//                        Rt            Ct > "now+-1d" = false, and will always be false
		//
		if rt.Before(ct) {
			return compareBoundaries(currBoundary, anchorTime.Add(ct.Sub(rt))), nil
		} else {
			// Is false, and always will be false no candidates
		}
	case lex.TokenLT, lex.TokenLE:
		// 3) ------ Ct -------------------     Ct < Rt
		//              Rt                      Ct < "now+-1d" = true (and always will be)
		//
		// 4) ----------- Ct --------------     Rt < Ct
		//     At----Rt                         Ct < "now+-1d" = true, but will be in true when at + (ct - rt)
		//           Bt---|
		//
		if ct.Before(rt) {
			// Is true, and always will be true no candidates
		} else {
			return compareBoundaries(currBoundary, anchorTime.Add(ct.Sub(rt))), nil
		}
	}
	return currBoundary, nil
}

// Boundary given all the date-maths in this node find the boundary time where
// this expression possibly will change boolean value.
// If no boundaries exist, returns time.Time{} (zero time)
func (d *DateConverter) Boundary() time.Time {
	return d.bt
}

var nowRegex = regexp.MustCompile(`^now([+-]+.*)*$`)

// Determine if this expression node uses datemath (ie, "now-4h")
func findDateMathFn(node expr.Node) BoundaryFns {
	fns := BoundaryFns{}
	switch n := node.(type) {
	case *expr.BinaryNode:
		for i, arg := range n.Args {
			switch narg := arg.(type) {
			case *expr.StringNode:
				val := strings.ToLower(narg.Text)

				if nowRegex.MatchString(val) {
					argIdx := i
					fns = append(fns, func(d *DateConverter, ctx expr.EvalIncludeContext) {
						// If left side is datemath   "now-3d" < ident then re-write to have ident on left
						var lhv value.Value
						op := n.Operator.T
						var ok bool
						if argIdx == 0 {
							lhv, ok = Eval(ctx, n.Args[1])
							if !ok {
								return
							}
							// Reverse equation to put identity on left side
							// "now-1d" < last_visit    =>   "last_visit" > "now-1d"
							switch n.Operator.T {
							case lex.TokenGT:
								op = lex.TokenLT
							case lex.TokenGE:
								op = lex.TokenLE
							case lex.TokenLT:
								op = lex.TokenGT
							case lex.TokenLE:
								op = lex.TokenGE
							default:
								// lex.TokenEqual, lex.TokenEqualEqual, lex.TokenNE:
								// none of these are supported operators for finding boundary
								return
							}
						} else if argIdx == 1 {
							lhv, ok = Eval(ctx, n.Args[0])
							if !ok {
								return
							}
						}
						d.bt, d.err = evalBoundary(d.at, d.bt, lhv, op, val)
						if d.err != nil {
							return
						}
					})
					continue
				}
			default:
				fns = append(fns, findDateMathFn(arg)...)
			}
		}

	case *expr.BooleanNode:
		for _, arg := range n.Args {
			fns = append(fns, findDateMathFn(arg)...)
		}
	case *expr.UnaryNode:
		return findDateMathFn(n.Arg)
	case *expr.TriNode:
		for _, arg := range n.Args {
			fns = append(fns, findDateMathFn(arg)...)
		}
	case *expr.FuncNode:
		for _, arg := range n.Args {
			fns = append(fns, findDateMathFn(arg)...)
		}
	case *expr.ArrayNode:
		for _, arg := range n.Args {
			fns = append(fns, findDateMathFn(arg)...)
		}
	case *expr.IncludeNode:
		// Assumes all includes are resolved
		if n.ExprNode != nil {
			return findDateMathFn(n.ExprNode)
		}
	case *expr.NumberNode, *expr.ValueNode, *expr.IdentityNode, *expr.StringNode:
		// Scalar/	Literal values cannot be datemath, must be binary-expression
	}
	return fns
}
