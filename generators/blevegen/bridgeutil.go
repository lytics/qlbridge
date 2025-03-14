package blevegen

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/generators/gentypes"
	"github.com/lytics/qlbridge/lex"
	"github.com/lytics/qlbridge/value"
)

type floatval interface {
	Float() float64
}

// makeRange returns a range query for Bleve given the 3 nodes that
// make up a comparison.
func makeRange(lhs *gentypes.FieldType, op lex.TokenType, rhs expr.Node) (query.Query, error) {
	rhsval, ok := scalar(rhs)
	if !ok {
		return nil, fmt.Errorf("unsupported type for comparison: %T", rhs)
	}

	rhv := value.NewValue(rhsval)

	// Convert scalars to correct type
	switch lhs.Type {
	case value.IntType, value.MapIntType:
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
		}
	}

	fieldName := lhs.Field

	// Create a range query
	rangeQuery := query.NewNumericRangeQuery(nil, nil)
	rangeQuery.SetField(fieldName)

	t := true
	f := false
	// Set the appropriate range boundaries based on the operator
	switch op {
	case lex.TokenGE:
		switch v := rhsval.(type) {
		case int:
			vf := float64(v)
			rangeQuery.Min = &vf
			rangeQuery.InclusiveMin = &t
		case int64:
			vf := float64(v)
			rangeQuery.Min = &vf
			rangeQuery.InclusiveMin = &t
		case float64:
			rangeQuery.Min = &v
			rangeQuery.InclusiveMin = &t
		}
	case lex.TokenLE:
		switch v := rhsval.(type) {
		case int:
			vf := float64(v)
			rangeQuery.Max = &vf
			rangeQuery.InclusiveMin = &t
		case int64:
			vf := float64(v)
			rangeQuery.Max = &vf
			rangeQuery.InclusiveMin = &t
		case float64:
			rangeQuery.Max = &v
			rangeQuery.InclusiveMin = &t
		}
	case lex.TokenGT:
		switch v := rhsval.(type) {
		case int:
			vf := float64(v)
			rangeQuery.Min = &vf
			rangeQuery.InclusiveMax = &f
		case int64:
			vf := float64(v)
			rangeQuery.Min = &vf
			rangeQuery.InclusiveMin = &f
		case float64:
			rangeQuery.Min = &v
			rangeQuery.InclusiveMin = &f
		}
	case lex.TokenLT:
		switch v := rhsval.(type) {
		case int:
			vf := float64(v)
			rangeQuery.Max = &vf
			rangeQuery.InclusiveMax = &f
		case int64:
			vf := float64(v)
			rangeQuery.Max = &vf
			rangeQuery.InclusiveMax = &f
		case float64:
			rangeQuery.Max = &v
			rangeQuery.InclusiveMax = &f
		}
	default:
		return nil, fmt.Errorf("qlindex: unsupported range operator %s", op)
	}

	// If the field is a date/time field, we need to handle it differently
	if lhs.Type == value.TimeType {
		// Convert to a date range query
		return makeDateRangeQuery(fieldName, op, rhsval)
	}

	return rangeQuery, nil
}

// makeDateRangeQuery creates a date range query for time-based fields
func makeDateRangeQuery(fieldName string, op lex.TokenType, rhsval interface{}) (query.Query, error) {
	var timeVal time.Time

	var ok bool
	timeVal, ok = value.ValueToTimeAnchor(value.NewValue(rhsval), time.Now())
	if !ok {
		return nil, fmt.Errorf("Could not convert %T %v to time.Time", rhsval, rhsval)
	}
	// Convert rhsval to time.Time
	// switch v := rhsval.(type) {
	// case time.Time:
	// 	timeVal = v
	// case string:
	// 	parsed, err := time.Parse(time.RFC3339, v)
	// 	if err != nil {
	// 		// Try other common formats
	// 		parsed, err = time.Parse("2006-01-02", v)
	// 		if err != nil {
	// 			return nil, fmt.Errorf("unable to parse time value: %v", v)
	// 		}
	// 	}
	// 	timeVal = parsed
	// default:
	// 	return nil, fmt.Errorf("unsupported type for date comparison: %T", rhsval)
	// }

	// Create a date range query
	dateRangeQuery := query.NewDateRangeQuery(time.Time{}, time.Time{})
	dateRangeQuery.SetField(fieldName)

	t := true
	f := false
	// Set the appropriate range boundaries based on the operator
	switch op {
	case lex.TokenGE:
		dateRangeQuery.Start = query.BleveQueryTime{Time: timeVal}
		dateRangeQuery.InclusiveStart = &t
	case lex.TokenLE:
		dateRangeQuery.End = query.BleveQueryTime{Time: timeVal}
		dateRangeQuery.InclusiveEnd = &t
	case lex.TokenGT:
		dateRangeQuery.Start = query.BleveQueryTime{Time: timeVal}
		dateRangeQuery.InclusiveStart = &f
	case lex.TokenLT:
		dateRangeQuery.End = query.BleveQueryTime{Time: timeVal}
		dateRangeQuery.InclusiveEnd = &f
	default:
		return nil, fmt.Errorf("qlindex: unsupported date range operator %s", op)
	}

	return dateRangeQuery, nil
}

// makeBetween returns a range query for Bleve given the 3 nodes that
// make up a between comparison.
func makeBetween(lhs *gentypes.FieldType, lower, upper interface{}) (query.Query, error) {
	fieldName := lhs.Field
	if lhs.Nested() {
		// Handle nested fields with dot notation for Bleve
		fieldName = strings.Replace(lhs.Path+"_"+lhs.Field, ".", "_", -1)
	}

	// Convert values based on the field type
	if lhs.Type == value.TimeType {
		// Handle date ranges
		return makeDateBetweenQuery(fieldName, lower, upper)
	}

	// Create a numeric range query
	rangeQuery := query.NewNumericRangeQuery(nil, nil)
	rangeQuery.SetField(fieldName)

	t := true
	// Set the boundaries
	switch l := lower.(type) {
	case int:
		lf := float64(l)
		rangeQuery.Min = &lf
		rangeQuery.InclusiveMin = &t
	case int64:
		lf := float64(l)
		rangeQuery.Min = &lf
		rangeQuery.InclusiveMin = &t
	case float64:
		rangeQuery.Min = &l
		rangeQuery.InclusiveMin = &t
	}

	switch u := upper.(type) {
	case int:
		uf := float64(u)
		rangeQuery.Max = &uf
		rangeQuery.InclusiveMax = &t
	case int64:
		uf := float64(u)
		rangeQuery.Max = &uf
		rangeQuery.InclusiveMax = &t
	case float64:
		rangeQuery.Max = &u
		rangeQuery.InclusiveMax = &t
	}

	return rangeQuery, nil
}

// makeDateBetweenQuery creates a date range query for BETWEEN operations on time fields
func makeDateBetweenQuery(fieldName string, lower, upper interface{}) (query.Query, error) {
	// Create a date range query
	dateRangeQuery := query.NewDateRangeQuery(time.Time{}, time.Time{})
	dateRangeQuery.SetField(fieldName)

	// Set the start time
	switch l := lower.(type) {
	case time.Time:
		dateRangeQuery.Start = query.BleveQueryTime{Time: l}
	case string:
		parsed, err := time.Parse(time.RFC3339, l)
		if err != nil {
			// Try other common formats
			parsed, err = time.Parse("2006-01-02", l)
			if err != nil {
				return nil, fmt.Errorf("unable to parse start time value: %v", l)
			}
		}
		dateRangeQuery.Start = query.BleveQueryTime{Time: parsed}
	default:
		return nil, fmt.Errorf("unsupported type for date range start: %T", lower)
	}
	t := true
	dateRangeQuery.InclusiveStart = &t

	// Set the end time
	switch u := upper.(type) {
	case time.Time:
		dateRangeQuery.End = query.BleveQueryTime{Time: u}
	case string:
		parsed, err := time.Parse(time.RFC3339, u)
		if err != nil {
			// Try other common formats
			parsed, err = time.Parse("2006-01-02", u)
			if err != nil {
				return nil, fmt.Errorf("unable to parse end time value: %v", u)
			}
		}
		dateRangeQuery.End = query.BleveQueryTime{Time: parsed}
	default:
		return nil, fmt.Errorf("unsupported type for date range end: %T", upper)
	}
	dateRangeQuery.InclusiveEnd = &t

	return dateRangeQuery, nil
}

// makeWildcard returns a wildcard/prefix query for Bleve
func makeWildcard(lhs *gentypes.FieldType, value string, addStars bool) (query.Query, error) {
	fieldName := lhs.Field
	// If we need to add wildcards (used for CONTAINS)
	if addStars {
		if !strings.HasPrefix(value, "*") {
			value = "*" + value
		}
		if !strings.HasSuffix(value, "*") {
			value = value + "*"
		}
		// Return a wildcard query
		wildcardQuery := query.NewWildcardQuery(value)
		wildcardQuery.SetField(fieldName)
		return wildcardQuery, nil
	}

	// For LIKE queries, we use the pattern directly
	// Bleve's wildcard query uses * and ? similar to SQL's LIKE % and _
	// Convert SQL LIKE pattern to Bleve wildcard pattern
	value = strings.Replace(value, "%", "*", -1)
	value = strings.Replace(value, "_", "?", -1)

	wildcardQuery := query.NewMatchQuery(value)
	wildcardQuery.SetField(fieldName)
	return wildcardQuery, nil
}

// makeTimeWindowQuery maps the provided threshold and window arguments to the indexed time buckets
func makeTimeWindowQuery(lhs *gentypes.FieldType, threshold, window, ts int64) (query.Query, error) {
	fieldName := lhs.Field
	if lhs.Nested() {
		// Handle nested fields with dot notation for Bleve
		fieldName = strings.Replace(lhs.Path+"_"+lhs.Field, ".", "_", -1)
	}

	// For Bleve, we need to create multiple conditions and combine them with a boolean query
	boolQuery := query.NewBooleanQuery(nil, nil, nil)

	// Add threshold condition
	thresholdQuery := query.NewTermQuery(strconv.FormatInt(threshold, 10))
	thresholdQuery.SetField(fieldName + "_threshold")
	boolQuery.AddMust(thresholdQuery)

	// Add window condition
	windowQuery := query.NewTermQuery(strconv.FormatInt(window, 10))
	windowQuery.SetField(fieldName + "_window")
	boolQuery.AddMust(windowQuery)

	t := true
	// Add time range conditions
	enterMax := float64(ts)
	enterRangeQuery := query.NewNumericRangeQuery(nil, &enterMax)
	enterRangeQuery.SetField(fieldName + "_enter")
	enterRangeQuery.InclusiveMax = &t
	boolQuery.AddMust(enterRangeQuery)

	exitMin := float64(ts)
	exitRangeQuery := query.NewNumericRangeQuery(&exitMin, nil)
	exitRangeQuery.SetField(fieldName + "_exit")
	exitRangeQuery.InclusiveMin = &t
	boolQuery.AddMust(exitRangeQuery)

	return boolQuery, nil
}
