package blevegen

import (
	"fmt"
	"strings"

	"github.com/blevesearch/bleve/v2/search/query"

	"github.com/lytics/qlbridge/generators/gentypes"
)

// Bleve's equivalent to match_all
func MatchAll() query.Query {
	return query.NewMatchAllQuery()
}

// Bleve's equivalent to match_none
func MatchNone() query.Query {
	return query.NewMatchNoneQuery()
}

// Term creates a new Bleve term query
func Term(fieldName string, value any) query.Query {
	termStr := fmt.Sprintf("%v", value)
	termQuery := query.NewTermQuery(termStr)
	termQuery.SetField(fieldName)
	return termQuery
}

// In creates a new Bleve disjunction query (OR) for multiple terms
func In(field *gentypes.FieldType, values []any) query.Query {
	fieldName := field.Field
	if field.Nested() {
		// Handle nested fields with dot notation for Bleve
		fieldName = strings.Replace(field.Path+"_"+field.Field, ".", "_", -1)
	}

	// Create a disjunction query (OR)
	disjQuery := query.NewDisjunctionQuery(nil)

	for _, val := range values {
		termStr := fmt.Sprintf("%v", val)
		termQuery := query.NewTermQuery(termStr)
		termQuery.SetField(fieldName)
		disjQuery.AddQuery(termQuery)
	}

	return disjQuery
}

// Exists creates a new Bleve query for field existence checks
func Exists(field *gentypes.FieldType) query.Query {
	fieldName := field.Field
	if field.Nested() {
		// Handle nested fields for Bleve
		fieldName = strings.Replace(field.Path+"_"+field.Field, ".", "_", -1)
	}

	// In Bleve, a field exists if it has any value
	wildcardQuery := query.NewWildcardQuery("*")
	wildcardQuery.SetField(fieldName)
	return wildcardQuery
}

// AndFilter creates a boolean query with multiple "must" clauses
func AndFilter(queries []query.Query) query.Query {
	boolQuery := query.NewBooleanQuery(nil, nil, nil)
	for _, q := range queries {
		boolQuery.AddMust(q)
	}
	return boolQuery
}

// OrFilter creates a boolean query with multiple "should" clauses
func OrFilter(queries []query.Query) query.Query {
	boolQuery := query.NewBooleanQuery(nil, nil, nil)
	for _, q := range queries {
		boolQuery.AddShould(q)
	}
	boolQuery.SetMinShould(1)
	return boolQuery
}

// NotFilter creates a boolean query with a "must not" clause
func NotFilter(q query.Query) query.Query {
	boolQuery := query.NewBooleanQuery(nil, nil, nil)
	boolQuery.AddMustNot(q)
	return boolQuery
}

// Wildcard creates a new Bleve wildcard query
func Wildcard(field, value string, addStars bool) query.Query {
	if addStars {
		if !strings.HasPrefix(value, "*") {
			value = "*" + value
		}
		if !strings.HasSuffix(value, "*") {
			value = value + "*"
		}
	}
	wildcardQuery := query.NewWildcardQuery(value)
	wildcardQuery.SetField(field)
	return wildcardQuery
}
