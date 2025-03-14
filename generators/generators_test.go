package generators_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/generators"
	"github.com/lytics/qlbridge/rel"
)

func TestSegmentQLIndexPlan(t *testing.T) {

	for _, backend := range []generators.SearchBackend{generators.BackendBleve, generators.BackendElasticsearch} {
		t.Run(fmt.Sprintf("Backend %v", backend), func(t *testing.T) {
			g := generators.NewGenerator(time.Now(), nil, nil, backend)

			fs, err := rel.ParseFilterQL(`FILTER x==1`)
			assert.Equal(t, nil, err)
			_, err = g.Walk(fs)
			assert.Equal(t, nil, err)
		})
	}
}

// TestIncluderNilReturn ensures that even if an Incuder returns (nil, nil),
// the generator will return an error and not panic.
//
// Includers should *not* return (nil, nil), but instead return an error when
// names cannot be resolved.
//
// Fixes #6169 #6176
func TestIncluderNilReturn(t *testing.T) {

	for _, backend := range []generators.SearchBackend{generators.BackendBleve, generators.BackendElasticsearch} {
		t.Run(fmt.Sprintf("Backend %v", backend), func(t *testing.T) {
			g := generators.NewGenerator(time.Now(), nilincluder{}, nil)
			fs, err := rel.ParseFilterQL(`FILTER INCLUDE xyz`)
			if err != nil {
				t.Fatalf("error parsing filterql: %v", err)
			}
			_, err = g.Walk(fs)
			if err == nil {
				t.Fatal("expected an error when using nilincluder but didn't receive one!")
			}
			t.Logf("expected error from ES Generator: %v", err)
		})
	}
}

type nilincluder struct{}

func (nilincluder) Include(name string) (expr.Node, error) { return nil, nil }
