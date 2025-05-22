package esgen

import (
	"encoding/json"
	"strings"

	"github.com/lytics/qlbridge/generators/gentypes"
)

/*
Native go data types that map to the Elasticsearch
Search DSL
*/
var _ = json.Marshal

type BoolFilter struct {
	Occurs         BoolOccurrence `json:"bool"`
	MinShouldMatch int            `json:"minimum_should_match,omitempty"`
}

type BoolOccurrence struct {
	Filter  []any `json:"filter,omitempty"`
	Should  []any `json:"should,omitempty"`
	MustNot any   `json:"must_not,omitempty"`
}

func AndFilter(v []any) *BoolFilter { return &BoolFilter{Occurs: BoolOccurrence{Filter: v}} }
func OrFilter(v []any) *BoolFilter  { return &BoolFilter{Occurs: BoolOccurrence{Should: v}} }
func NotFilter(v any) *BoolFilter   { return &BoolFilter{Occurs: BoolOccurrence{MustNot: v}} }

// Filter structs

type exists struct {
	Exists map[string]string `json:"exists"`
}

// Exists creates a new Elasticsearch filter {"exists": {"field": field}}
func Exists(field *gentypes.FieldType) any {
	if field.Nested() {
		/*
			"nested": {
				"query": {
				    "term": {
				        "map_actioncounts.k": "Web hit"
				    }
				},
				"path": "map_actioncounts"
			}
		*/
		return &nested{&NestedQuery{
			Query:          Term(field.Path+".k", field.Field),
			Path:           field.Path,
			IgnoreUnmapped: true,
		}}
		//Nested(field.Path, &term{map[string][]string{"k": field.Field}})
	}
	return &exists{map[string]string{"field": field.Field}}
}

//	type and struct {
//		Filters []interface{} `json:"and"`
//	}
type boolean struct {
	Bool any `json:"bool"`
}
type must struct {
	Filters []any `json:"must"`
}

type in struct {
	Terms map[string][]any `json:"terms"`
}

// In creates a new Elasticsearch terms filter
//
// {"terms": {field: values}}
//
//	{ "nested": {
//	     "query": {
//	        "bool" : {
//	           "must" :[
//	              {"term": { "k":fieldName}},
//	              filter,
//	           ]
//	     } ,
//	     "path":"path_to_obj"
//	 }}
func In(field *gentypes.FieldType, values []any) any {
	if field.Nested() {
		return &nested{&NestedQuery{
			Query: &boolean{
				&must{
					Filters: []any{
						&in{map[string][]any{field.PathAndPrefix(""): values}},
						Term(field.Path+".k", field.Field),
					},
				},
			},
			Path:           field.Path,
			IgnoreUnmapped: true,
		}}
	}
	return &in{map[string][]any{field.Field: values}}
}

// Nested creates a new Elasticsearch nested filter
//
//	{ "nested": {
//	     "query": {
//	        "bool" : {
//	           "must" :[
//	              {"term": { "k":fieldName}},
//	              filter,
//	           ]
//	     } ,
//	     "path":"path_to_obj"
//	 }}
func Nested(field *gentypes.FieldType, filter any) *nested {

	// Hm.  Elasticsearch doc seems to insinuate we don't need
	// this path + ".k" but unit tests say otherwise
	fl := []any{
		Term(field.Path+".k", field.Field),
		filter,
	}
	n := nested{&NestedQuery{
		Query:          &boolean{&must{fl}},
		Path:           field.Path,
		IgnoreUnmapped: true,
	}}
	// by, _ := json.MarshalIndent(n, "", "  ")
	// u.Infof("NESTED4:  \n%s", string(by))
	return &n
}

type nested struct {
	Nested *NestedQuery `json:"nested,omitempty"`
}

type NestedQuery struct {
	Query          any    `json:"query"`
	Path           string `json:"path"`
	IgnoreUnmapped bool   `json:"ignore_unmapped,omitempty"`
}

type RangeQry struct {
	GTE any `json:"gte,omitempty"`
	LTE any `json:"lte,omitempty"`
	GT  any `json:"gt,omitempty"`
	LT  any `json:"lt,omitempty"`
}

type RangeFilter struct {
	Range map[string]RangeQry `json:"range"`
}

type term struct {
	Term map[string]any `json:"term"`
}

// Term creates a new Elasticsearch term filter {"term": {field: value}}
func Term(fieldName string, value any) *term {
	return &term{map[string]any{fieldName: value}}
}

type matchall struct {
	MatchAll *struct{} `json:"match_all"`
}

// MatchAll maps to the Elasticsearch "match_all" filter
var MatchAll = &matchall{&struct{}{}}

// MatchNone matches no documents.
var MatchNone = NotFilter(MatchAll)

type wildcard struct {
	Wildcard map[string]string `json:"wildcard"`
}

func wcFunc(val string, addStars bool) string {
	if len(val) < 1 || !addStars {
		return val
	}
	if val[0] == '*' || val[len(val)-1] == '*' {
		return val
	}
	if !strings.HasPrefix(val, "*") {
		val = "*" + val
	}
	if !strings.HasSuffix(val, "*") {
		val = val + "*"
	}
	return val
}

// Wilcard creates a new Elasticserach wildcard query
//
//	{"wildcard": {field: value}}
//
// nested
//
//	{"nested": {
//	   "filter" : { "and" : [
//	           {"wildcard": {"v": value}},
//	           {"term":{"k": field_key}}
//	   "path": path
//	  }
//	}
func Wildcard(field, value string, addStars bool) *wildcard {
	return &wildcard{Wildcard: map[string]string{field: wcFunc(value, addStars)}}
}

type GeoDistanceFilter struct {
	GeoDistance map[string]any `json:"geo_distance"`
}
