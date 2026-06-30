package esgen

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/generators/gentypes"
	"github.com/lytics/qlbridge/lex"
	"github.com/lytics/qlbridge/value"
)

type floatval interface {
	Float() float64
}

// makeRange returns a range filter for Elasticsearch given the 3 nodes that
// make up a comparison.
func makeRange(lhs *gentypes.FieldType, op lex.TokenType, rhs expr.Node) (any, error) {

	rhsval, ok := scalar(rhs)
	if !ok {
		return nil, fmt.Errorf("unsupported type for comparison: %T", rhs)
	}

	rhv := value.NewValue(rhsval)

	// Convert scalars to correct type
	switch lhs.Type {
	case value.IntType, value.MapIntType:
		// TODO:  we might need to change the operator???
		//  given lh identity "purchase_count" = int = 10
		//  right hand side = float 9.7
		iv, ok := value.ValueToInt64(rhv)
		if !ok {
			return nil, fmt.Errorf("Could not convert %T %v to int", rhsval, rhsval)
		}
		rhsval = iv
	case value.NumberType, value.MapNumberType:
		fv, ok := value.ValueToFloat64(rhv)
		if !ok {
			return nil, fmt.Errorf("Could not convert %T %v to float", rhsval, rhsval)
		}
		rhsval = fv
	default:
		if rhsstr, ok := rhsval.(string); ok {
			if rhsf, err := strconv.ParseFloat(rhsstr, 64); err == nil {
				// rhsval can be converted to a float!
				rhsval = rhsf
			}
			// ISO date string → epoch millis float.
			if t, err := dateparse.ParseAny(rhsstr); err == nil {
				rhsval = float64(t.UnixMilli())
			}
		}
	}

	/*
		"nested": {
			"query": {
			    "term": {
			        "map_actioncounts.k": "Web hit"
			    }
			},
			"path": "map_actioncounts"
		}

		"nested": {
			"query": {
			    "bool": {
			      "must": [
			          {
			              "term": {
			                  "mapvals_fields.k": "has_data"
			              }
			          },
			          {
			              "term": {
			                  "mapvals_fields.b": true
			              }
			          }
			      ]
			    }
			},
			"path": "mapvals_fields"
		}
		"nested": {
			"query": {
				"bool": {
					"must": [
						{
							"term": {
								"k": "open"
							}
						},
						{
							"range": {
								"f": {"gte": 7}
							}
						}
					]
				}
			},
			"path": "map_events"
		}
		q = esMap{"nested": esMap{"path": parent, "filter": esMap{"and": []esMap{
					{"term": esMap{parent + ".k": child}},
					{"range": esMap{parent + valuePath: esMap{esRangeOps[seg.SegType]: rhsNum}}},
				}}}}
	*/

	fieldName := lhs.Field
	if lhs.Nested() {
		fieldName, rhsval = lhs.PrefixAndValue(rhsval)
	}
	r := &RangeFilter{}
	switch op {
	case lex.TokenGE:
		r.Range = map[string]RangeQry{fieldName: {GTE: rhsval}}
	case lex.TokenLE:
		r.Range = map[string]RangeQry{fieldName: {LTE: rhsval}}
	case lex.TokenGT:
		r.Range = map[string]RangeQry{fieldName: {GT: rhsval}}
	case lex.TokenLT:
		r.Range = map[string]RangeQry{fieldName: {LT: rhsval}}
	default:
		return nil, fmt.Errorf("qlindex: unsupported range operator %s", op)
	}
	if lhs.Nested() {
		return Nested(lhs, r), nil
	}
	return r, nil
}

// makeRecurringQuery builds a Painless script matching documents whose date
// field lands on a recurrence of itself relative to `now`. `period` is either a
// string ("yearly"/"monthly"/"weekly"/"daily") or an integer node (every N
// days). offsetDays shifts the recurrence (e.g. yearly+182 = half-birthday).
func makeRecurringQuery(lhs *gentypes.FieldType, period expr.Node, offsetDays int, now time.Time) (any, error) {
	if lhs.Nested() {
		return nil, fmt.Errorf("'recurring' unsupported for nested/map field %q", lhs.Field)
	}
	if lhs.Type != value.TimeType {
		return nil, fmt.Errorf("'recurring' requires a date field, got %s for %q", lhs.Type, lhs.Field)
	}

	q := painlessQuote(lhs.Field)
	exists := fmt.Sprintf("doc[%s].size() != 0", q)

	// Numeric period => every N days.
	if num, ok := period.(*expr.NumberNode); ok {
		if !num.IsInt || num.Int64 <= 0 {
			return nil, fmt.Errorf("'recurring' day period must be a positive integer, got %v", period)
		}
		n := num.Int64
		todayDay := now.UTC().Unix() / 86400
		src := fmt.Sprintf("if (%s) { long d = params.todayDay - (doc[%s].value.toInstant().getEpochSecond() / 86400) - params.offset; return d >= 0 && d %% params.n == 0; } return false;",
			exists, q)
		return Script(src, map[string]any{"todayDay": todayDay, "offset": offsetDays, "n": n}), nil
	}

	pnode, ok := period.(*expr.StringNode)
	if !ok {
		return nil, fmt.Errorf("'recurring' period must be a string or integer, got %s", period.NodeType())
	}

	target := now.UTC().AddDate(0, 0, -offsetDays)
	switch strings.ToLower(pnode.Text) {
	case "yearly", "annual", "annually":
		src := fmt.Sprintf("%s && doc[%s].value.getMonthValue() == params.month && doc[%s].value.getDayOfMonth() == params.day", exists, q, q)
		return Script(src, map[string]any{"month": int(target.Month()), "day": target.Day()}), nil
	case "monthly":
		src := fmt.Sprintf("%s && doc[%s].value.getDayOfMonth() == params.day", exists, q)
		return Script(src, map[string]any{"day": target.Day()}), nil
	case "weekly":
		src := fmt.Sprintf("%s && doc[%s].value.getDayOfWeek().getValue() == params.dow", exists, q)
		return Script(src, map[string]any{"dow": isoDayOfWeek(target)}), nil
	case "daily":
		return Script(exists, nil), nil
	default:
		return nil, fmt.Errorf("'recurring' unsupported period %q (want yearly/monthly/weekly/daily or an integer)", pnode.Text)
	}
}

// isoDayOfWeek maps Go's Weekday (Sunday=0) to ISO-8601 (Monday=1..Sunday=7),
// matching Painless's ZonedDateTime.getDayOfWeek().getValue().
func isoDayOfWeek(t time.Time) int {
	if wd := int(t.Weekday()); wd != 0 {
		return wd
	}
	return 7
}

// makeFieldRange compares two document fields (e.g. `a < b`). Elasticsearch
// range queries can only compare a field to a constant, so a field-to-field
// comparison is expressed as a Painless script.
func makeFieldRange(lhs *gentypes.FieldType, op lex.TokenType, rhs *gentypes.FieldType) (any, error) {
	if lhs.Nested() || rhs.Nested() {
		return nil, fmt.Errorf("field-to-field comparison unsupported for nested/map fields: %q, %q", lhs.Field, rhs.Field)
	}
	if painlessTypeClass(lhs.Type) != painlessTypeClass(rhs.Type) {
		return nil, fmt.Errorf("field-to-field comparison requires comparable types, got %s and %s", lhs.Type, rhs.Type)
	}

	cmp, err := painlessCompareOp(op)
	if err != nil {
		return nil, err
	}
	laccess, err := painlessFieldAccess(lhs)
	if err != nil {
		return nil, err
	}
	raccess, err := painlessFieldAccess(rhs)
	if err != nil {
		return nil, err
	}

	src := fmt.Sprintf("doc[%s].size() != 0 && doc[%s].size() != 0 && %s %s %s",
		painlessQuote(lhs.Field), painlessQuote(rhs.Field), laccess, cmp, raccess)
	return Script(src, nil), nil
}

func painlessCompareOp(op lex.TokenType) (string, error) {
	switch op {
	case lex.TokenGE:
		return ">=", nil
	case lex.TokenLE:
		return "<=", nil
	case lex.TokenGT:
		return ">", nil
	case lex.TokenLT:
		return "<", nil
	default:
		return "", fmt.Errorf("qlindex: unsupported range operator %s", op)
	}
}

// painlessTypeClass groups field types that are mutually comparable.
func painlessTypeClass(t value.ValueType) string {
	switch t {
	case value.TimeType:
		return "time"
	case value.IntType, value.NumberType:
		return "number"
	default:
		return "unsupported"
	}
}

func painlessFieldAccess(f *gentypes.FieldType) (string, error) {
	q := painlessQuote(f.Field)
	switch f.Type {
	case value.TimeType:
		return fmt.Sprintf("doc[%s].value.toInstant().toEpochMilli()", q), nil
	case value.IntType, value.NumberType:
		return fmt.Sprintf("doc[%s].value", q), nil
	default:
		return "", fmt.Errorf("field-to-field comparison unsupported for type %s field %q", f.Type, f.Field)
	}
}

// painlessQuote returns a single-quoted Painless string literal.
func painlessQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}

// makeBetween returns a range filter for Elasticsearch given the 3 nodes that
// make up a comparison.
func makeBetween(lhs *gentypes.FieldType, lower, upper any) (any, error) {
	lower = coerceScalar(lhs, lower)
	upper = coerceScalar(lhs, upper)

	fieldName := lhs.Field
	if lhs.Nested() {
		fieldName, lower = lhs.PrefixAndValue(lower)
		_, upper = lhs.PrefixAndValue(upper)
	}

	lr := &RangeFilter{Range: map[string]RangeQry{fieldName: {GT: lower}}}
	ur := &RangeFilter{Range: map[string]RangeQry{fieldName: {LT: upper}}}
	inner := &boolean{must{[]any{lr, ur}}}

	if lhs.Nested() {
		return Nested(lhs, inner), nil
	}
	return inner, nil
}

// coerceScalar converts a scalar value to the appropriate Go type for the given
// field type before embedding it in an Elasticsearch range query. This mirrors
// the coercion makeRange performs so that BETWEEN and comparison operators
// produce consistent queries.
//
// For IntType fields the value is converted to int64. For NumberType fields it
// is converted to float64. For all other types (including TimeType) string
// values are first tried as a float (epoch-millis strings like "1778310000000")
// and then as an ISO date string (e.g. "2026-05-09"), which is converted to
// epoch milliseconds. Values that cannot be coerced are returned unchanged so
// that Elasticsearch can attempt its own parsing.
func coerceScalar(lhs *gentypes.FieldType, val any) any {
	rhv := value.NewValue(val)
	switch lhs.Type {
	case value.IntType, value.MapIntType:
		if iv, ok := value.ValueToInt64(rhv); ok {
			return iv
		}
	case value.NumberType, value.MapNumberType:
		if fv, ok := value.ValueToFloat64(rhv); ok {
			return fv
		}
	default:
		s, ok := val.(string)
		if !ok {
			return val
		}
		// Numeric string → epoch millis float.
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return f
		}
		// ISO date string → epoch millis float.
		if t, err := dateparse.ParseAny(s); err == nil {
			return float64(t.UnixMilli())
		}
	}
	return val
}

// makeWildcard returns a wildcard/like query
//
//	{"wildcard": {field: value}}
func makeWildcard(lhs *gentypes.FieldType, value string, addStars bool) (any, error) {
	/*
		"nested": {
			"query": {
				"bool": {
					"must": [
						{
							"term": { "map_events.k": "open" }
						},
						{
							"wildcard": {"map_events.v": "hel"}
						}
					]
				}
			},
			"path": "map_events"
		}

		{"wildcard": {field: value}}
	*/
	fieldName := lhs.Field

	if lhs.Nested() {
		fieldName = lhs.PathAndPrefix(value)
	}
	wc := Wildcard(fieldName, value, addStars)
	if lhs.Nested() {
		fl := []any{wc, Term(fmt.Sprintf("%s.k", lhs.Path), lhs.Field)}
		return &nested{&NestedQuery{
			Query:          &boolean{must{fl}},
			Path:           lhs.Path,
			IgnoreUnmapped: true,
		}}, nil
	}
	return &wc, nil
}

// makeTimeWindowQuery maps the provided threshold and window arguments to the indexed time buckets
func makeTimeWindowQuery(lhs *gentypes.FieldType, threshold, window, ts int64) (any, error) {
	/*
		"nested": {
			"query": {
			  "bool":{
				"must": [
					{
						"term": { "timebucket_visits.threshold": 1 }
					},
					{
						"term": { "timebucket_visits.window": 3 }
					},
					{
						"range": {
							"timebucket_visits.enter: { "lte": 16916 }
						}
					},
					{
						"range": {
							"timebucket_visits.exit: { "gte": 16916 }
						}
					},
				]
			  }
			}
			"path": "timebucket_visits"
		}
	*/

	fl := []any{
		Term(lhs.Field+".threshold", strconv.FormatInt(threshold, 10)),
		Term(lhs.Field+".window", strconv.FormatInt(window, 10)),
		&RangeFilter{Range: map[string]RangeQry{lhs.Field + ".enter": {LTE: ts}}},
		&RangeFilter{Range: map[string]RangeQry{lhs.Field + ".exit": {GTE: ts}}},
	}

	return &nested{&NestedQuery{
		Query:          &boolean{must{fl}},
		Path:           lhs.Field,
		IgnoreUnmapped: true,
	}}, nil
}
