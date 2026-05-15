package esgen

import (
	"fmt"
	"strconv"

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
