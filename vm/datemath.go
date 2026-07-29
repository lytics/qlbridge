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

func FindBoundary(anchorTime time.Time, ctx expr.EvalContext, includer expr.Includer, fns BoundaryFns) (time.Time, error) {
	dc := &DateConverter{
		at: anchorTime,
	}
	for _, fn := range fns {
		fn(dc, ctx, includer)
	}
	return dc.bt, dc.err
}

type BoundaryFns []func(*DateConverter, expr.EvalContext, expr.Includer)

func CalcBoundaryFns(n expr.Node) BoundaryFns {
	return findDateMathFn(n)
}

func NewDateConverterWithAnchorTime(ctx expr.EvalContext, includer expr.Includer, n expr.Node, at time.Time) (*DateConverter, error) {
	dc := &DateConverter{
		at: at,
	}
	fns := findDateMathFn(n)
	dc.bt, dc.err = FindBoundary(dc.at, ctx, includer, fns)
	if dc.err == nil && len(fns) > 0 {
		dc.HasDateMath = true
	}
	return dc, dc.err
}

// NewDateConverter takes a node expression
func NewDateConverter(ctx expr.EvalContext, inc expr.Includer, n expr.Node) (*DateConverter, error) {
	return NewDateConverterWithAnchorTime(ctx, inc, n, time.Now())
}
func compareBoundaries(currBoundary, newBoundary time.Time) time.Time {
	// Should we check for is zero on the newBoundary?
	if currBoundary.IsZero() || newBoundary.Before(currBoundary) {
		return newBoundary
	}
	return currBoundary
}
func evalBoundary(anchorTime, currBoundary time.Time, lhv value.Value, op lex.TokenType, val string) (time.Time, error) {
	ct, ok := value.ValueToTime(lhv)
	if !ok {
		return currBoundary, fmt.Errorf("Could not convert %T: %v to time.Time", lhv, lhv)
	}

	// Given Anchor Time At calculate Relative Time Rt
	rt, err := datemath.EvalAnchor(anchorTime, val)
	if err != nil {
		return currBoundary, err
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
					fns = append(fns, func(d *DateConverter, ctx expr.EvalContext, inc expr.Includer) {
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
		// recurring() is now-relative without containing any datemath literal, so
		// it needs its own boundary rather than a scan of its args.
		if strings.EqualFold(n.Name, "recurring") {
			if fn := findBoundaryForRecurring(n); fn != nil {
				return BoundaryFns{fn}
			}
			return fns
		}
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
func findBoundaryForBetween(n *expr.TriNode) func(d *DateConverter, ctx expr.EvalContext, inc expr.Includer) {

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

	return func(d *DateConverter, ctx expr.EvalContext, inc expr.Includer) {

		lhv, ok := EvalInc(inc, ctx, arg1)
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

// findBoundaryForRecurring builds the boundary fn for
// recurring(date_field, period[, offsetDays]). Unlike the datemath cases there is
// no "now±N" literal to invert: the expression is true for whole UTC days and
// flips at midnight, so the boundary is the next day on which its value changes.
// Returns nil for shapes the evaluators reject, so those don't get flagged as
// needing recalculation.
func findBoundaryForRecurring(n *expr.FuncNode) func(d *DateConverter, ctx expr.EvalContext, inc expr.Includer) {
	if len(n.Args) < 2 || len(n.Args) > 3 {
		return nil
	}
	if _, ok := n.Args[0].(*expr.IdentityNode); !ok {
		return nil
	}

	var period string
	var nDays int
	switch pn := n.Args[1].(type) {
	case *expr.StringNode:
		period = strings.ToLower(pn.Text)
		switch period {
		case "yearly", "monthly", "weekly":
		default:
			return nil
		}
	case *expr.NumberNode:
		if !pn.IsInt || pn.Int64 <= 0 {
			return nil
		}
		nDays = int(pn.Int64)
	default:
		return nil
	}

	offsetDays := 0
	if len(n.Args) == 3 {
		on, ok := n.Args[2].(*expr.NumberNode)
		if !ok || !on.IsInt {
			return nil
		}
		offsetDays = int(on.Int64)
	}

	anchorNode := n.Args[0]
	return func(d *DateConverter, ctx expr.EvalContext, inc expr.Includer) {
		lhv, ok := EvalInc(inc, ctx, anchorNode)
		if !ok {
			return
		}
		anchor, ok := value.ValueToTime(lhv)
		if !ok || anchor.IsZero() {
			// No anchor date means the expression can't become true for this row.
			return
		}
		if bt := RecurringBoundary(anchor, d.at, period, nDays, offsetDays); !bt.IsZero() {
			d.bt = compareBoundaries(d.bt, bt)
		}
	}
}

// RecurringBoundary returns the next UTC midnight at which recurring(anchor,
// period, offsetDays) changes value relative to `now`, or the zero time when it
// never changes again. Mirrors the evaluators: with n > 0 the recurrence is every
// n days from the anchor, otherwise period selects yearly/monthly/weekly.
func RecurringBoundary(anchor, now time.Time, period string, n, offsetDays int) time.Time {
	if n > 0 {
		return recurringBoundaryNDays(anchor, now, n, offsetDays)
	}
	return recurringBoundaryPeriod(anchor, now, period, offsetDays)
}

// recurringBoundaryNDays works in epoch-days, flooring toward negative infinity
// so a pre-1970 anchor buckets by calendar day. Matches the every-n-days
// evaluators, which floor the same way.
func recurringBoundaryNDays(anchor, now time.Time, n, offsetDays int) time.Time {
	anchorDay := floorDivInt64(anchor.UTC().Unix(), secondsPerDay)
	nowDay := floorDivInt64(now.UTC().Unix(), secondsPerDay)
	// The evaluator subtracts the offset from the day difference, so the first
	// matching day sits offsetDays after the anchor.
	first := anchorDay + int64(offsetDays)

	if nowDay < first {
		return dayStartFromEpochDay(first)
	}
	if n == 1 {
		// Every day from `first` onward matches, so it never flips back.
		return time.Time{}
	}
	if rem := (nowDay - first) % int64(n); rem != 0 {
		return dayStartFromEpochDay(nowDay + int64(n) - rem)
	}
	// Matches today; goes false at the start of tomorrow.
	return dayStartFromEpochDay(nowDay + 1)
}

// recurringBoundaryPeriod handles yearly/monthly/weekly, which the evaluators
// compare on the calendar date of now-offsetDays.
func recurringBoundaryPeriod(anchor, now time.Time, period string, offsetDays int) time.Time {
	anchorU := anchor.UTC()
	today := dayStart(now.UTC())
	target := today.AddDate(0, 0, -offsetDays)

	next := nextPeriodRecurrence(anchorU, target, period)
	if next.IsZero() {
		return time.Time{}
	}
	if next.After(target) {
		// Goes true at the start of that day, shifted back into "now" space.
		return next.AddDate(0, 0, offsetDays)
	}
	// Matches today. yearly/monthly/weekly recurrences are never on consecutive
	// days, so it goes false at the start of tomorrow.
	return today.AddDate(0, 0, 1)
}

// nextPeriodRecurrence returns the first UTC day on or after `from` whose
// calendar date is a recurrence of anchor. Anchors with no counterpart in a given
// period are skipped rather than clamped -- Feb 29 only recurs in leap years, and
// the 31st only in months that have one -- matching the evaluators.
func nextPeriodRecurrence(anchor, from time.Time, period string) time.Time {
	switch period {
	case "yearly":
		// A Feb-29 anchor can skip up to 7 years across a non-leap century.
		for y := from.Year(); y <= from.Year()+8; y++ {
			c := time.Date(y, anchor.Month(), anchor.Day(), 0, 0, 0, 0, time.UTC)
			if c.Month() != anchor.Month() || c.Day() != anchor.Day() {
				continue
			}
			if !c.Before(from) {
				return c
			}
		}
	case "monthly":
		first := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.UTC)
		for i := 0; i < 14; i++ {
			m := first.AddDate(0, i, 0)
			c := time.Date(m.Year(), m.Month(), anchor.Day(), 0, 0, 0, 0, time.UTC)
			if c.Month() != m.Month() {
				continue
			}
			if !c.Before(from) {
				return c
			}
		}
	case "weekly":
		delta := (int(anchor.Weekday()) - int(from.Weekday()) + 7) % 7
		return from.AddDate(0, 0, delta)
	}
	return time.Time{}
}

const secondsPerDay = 86400

func dayStart(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func dayStartFromEpochDay(day int64) time.Time {
	return time.Unix(day*secondsPerDay, 0).UTC()
}

// floorDivInt64 mirrors Painless's Math.floorDiv: rounds toward negative
// infinity instead of toward zero.
func floorDivInt64(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}
