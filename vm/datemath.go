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

func NewDateConverterWithAnchorTime(ctx expr.EvalIncludeContext, n expr.Node, at time.Time) (*DateConverter, error) {
	dc := &DateConverter{
		at: at,
	}
	fns := findDateMathFn(n)
	dc.bt, dc.err = FindBoundary(dc.at, ctx, fns)
	if dc.err == nil && len(fns) > 0 {
		dc.HasDateMath = true
	}
	return dc, dc.err
}

// NewDateConverter takes a node expression
func NewDateConverter(ctx expr.EvalIncludeContext, n expr.Node) (*DateConverter, error) {
	return NewDateConverterWithAnchorTime(ctx, n, time.Now())
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
		// Only handle BETWEEN operator with specific node types
		if n.Operator.T == lex.TokenBetween && len(n.Args) == 3 {
			// Check if first arg is IdentityNode and other two are StringNodes
			fn := findBoundaryForBetween(n)
			if fn != nil {
				fns = append(fns, fn)
				return fns
			}
		}

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

// findBoundaryForBetween calculates the next time boundary for a BETWEEN expression
// with date math boundaries. It handles expressions like:
//
//	time_column BETWEEN "now-3d" AND "now+3d"
//
// The function returns a boundary function that:
// 1. Evaluates the comparison time (Ct) against the window boundaries
// 2. Determines when the expression's boolean value will change
// 3. Returns the appropriate re-evaluation time
//
// Example:
//
//	Input:  time_column BETWEEN "now-3d" AND "now+3d"
//	When:   now = 2025-01-22
//	Window: 2025-01-19 to 2025-01-25
//
//	If Ct = 2025-01-01 (left side of window):
//	- Expression is false
//	- Will always be false as window is moving forward
//	- Returns zero time (no re-evaluation needed)
//
//	If Ct = 2025-01-30 (right side of window):
//	- Expression is false
//	- Will become true when window catches up (enter event)
//	- Returns re-evaluation time when this will enter the window
//
//	If Ct = 2025-01-22 (inside window):
//	- Expression is true
//	- Will become false when Ct passes lower bound (exit event)
//	- Returns re-evaluation time when this will be exit the window
func findBoundaryForBetween(n *expr.TriNode) func(d *DateConverter, ctx expr.EvalIncludeContext) {

	// Check if first arg is IdentityNode and other two are StringNodes
	_, isFirstIdentity := n.Args[0].(*expr.IdentityNode)
	_, isSecondString := n.Args[1].(*expr.StringNode)
	_, isThirdString := n.Args[2].(*expr.StringNode)

	if !isFirstIdentity || !isSecondString || !isThirdString {
		return nil
	}
	arg1, arg2, arg3 := n.Args[0], n.Args[1], n.Args[2]

	// datemath only if both date args are relative to an anchor time like "now-1d"
	val2 := strings.ToLower(arg2.(*expr.StringNode).Text)
	val3 := strings.ToLower(arg3.(*expr.StringNode).Text)
	if !nowRegex.MatchString(val2) || !nowRegex.MatchString(val3) {
		return nil
	}

	return func(d *DateConverter, ctx expr.EvalIncludeContext) {

		lhv, ok := Eval(ctx, arg1)
		if !ok {
			return
		}
		ct, ok := value.ValueToTime(lhv)
		if !ok {
			d.err = fmt.Errorf("could not convert %T: %v to time.Time", lhv, lhv)
			return
		}

		date1, err := datemath.EvalAnchor(d.at, val2)
		if err != nil {
			d.err = err
			return
		}

		date2, err := datemath.EvalAnchor(d.at, val3)
		if err != nil {
			d.err = err
			return
		}

		// assign lower and upper bounds
		lower, upper := date1, date2
		if date1.After(date2) {
			lower, upper = date2, date1
		}

		if ct.Before(lower) {
			// out of window's lower bound, so will always be false
			return
		}

		if ct.After(upper) || ct.Equal(upper) {
			// in the future or right in the border, so will enter the window later sometime in the future, do re-evaluate
			d.bt = compareBoundaries(d.bt, d.at.Add(ct.Sub(upper)))
			return
		}
		// currently in the window, so will exit the window in the future, do re-evaluate
		d.bt = compareBoundaries(d.bt, d.at.Add(ct.Sub(lower)))
	}
}
