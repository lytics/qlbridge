package generators

import (
	"time"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/generators/blevegen"
	"github.com/lytics/qlbridge/generators/esgen"
	"github.com/lytics/qlbridge/generators/gentypes"
)

type (
	// Generator interface accepts a FilterStatement and walks
	// the ast statement generating an elasticsearch payload
	Generator interface {
		WalkExpr(stmt expr.Node) (*gentypes.Payload, error)
	}

	// SearchBackend indicates which search engine to generate queries for
	SearchBackend int
)

func (b SearchBackend) String() string {
	switch b {
	case BackendElasticsearch:
		return "elasticsearch"
	case BackendBleve:
		return "bleve"
	}
	return "unknown"
}

const (
	// BackendElasticsearch generates queries for Elasticsearch
	BackendElasticsearch SearchBackend = iota
	// BackendBleve generates queries for Bleve
	BackendBleve
)

// NewGenerator creates a new query generator for the specified backend
func NewGenerator(ts time.Time, inc expr.Includer, mapper gentypes.SchemaColumns, opts ...any) Generator {
	backend := BackendElasticsearch
	if len(opts) > 0 {
		optBackend, ok := opts[len(opts)-1].(SearchBackend)
		if ok {
			backend = optBackend
		}
	}
	switch backend {
	case BackendBleve:
		return blevegen.NewGenerator(ts, inc, mapper)
	default: // Default to Elasticsearch
		return esgen.NewGenerator(ts, inc, mapper)
	}
}
