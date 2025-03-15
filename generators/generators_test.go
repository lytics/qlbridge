package generators_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/generators"
	"github.com/lytics/qlbridge/rel"
	"github.com/stretchr/testify/require"
)

func TestSegmentQLIndexPlan(t *testing.T) {
	for _, backend := range []generators.SearchBackend{generators.BackendBleve, generators.BackendElasticsearch} {
		t.Run(fmt.Sprintf("Backend %s", backend), func(t *testing.T) {
			g := generators.NewGenerator(time.Now(), nil, nil, backend)
			fs, err := rel.ParseFilterQL(`FILTER x==1`)
			require.Equal(t, nil, err)
			_, err = g.WalkExpr(fs.Filter)
			require.Equal(t, nil, err)
		})
	}
}

// TestIncluderNilReturn ensures that even if an Incuder returns (nil, nil),
// the generator will return an error and not panic.
//
// Includers should *not* return (nil, nil), but instead return an error when
// names cannot be resolved.
func TestIncluderNilReturn(t *testing.T) {
	for _, backend := range []generators.SearchBackend{generators.BackendBleve, generators.BackendElasticsearch} {
		t.Run(fmt.Sprintf("Backend %v", backend), func(t *testing.T) {
			g := generators.NewGenerator(time.Now(), nilincluder{}, nil)
			fs, err := rel.ParseFilterQL(`FILTER INCLUDE xyz`)
			require.NoError(t, err)
			_, err = g.WalkExpr(fs.Filter)
			require.Error(t, err)
		})
	}
}

type nilincluder struct{}

func (nilincluder) Include(name string) (expr.Node, error) { return nil, nil }
