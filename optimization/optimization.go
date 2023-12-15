package optimization

import (
	"sort"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/vm"
)

// SharedIncludedNodes stores nodes of already optimized included subtrees.
// Its main purpose is to reduce memory usage of optimized expression trees by sharing these nodes.
type SharedIncludedNodes struct {
	data map[string]*nodeWithNumberOfChildren
}

func NewSharedIncludedNodes() *SharedIncludedNodes {
	return &SharedIncludedNodes{
		data: make(map[string]*nodeWithNumberOfChildren),
	}
}

type nodeWithNumberOfChildren struct {
	numberOfChildren uint64
	node             expr.Node
}

// OptimizeBooleanNodes optimizes boolean nodes in the expression tree by sorting their arguments by number of children.
// It returns an optimized deep-copy of the expression tree in order not to violate the immutability of expression trees.
func OptimizeBooleanNodes(ctx expr.Includer, arg expr.Node, sharedIncludedNodes *SharedIncludedNodes) (expr.Node, error) {
	newNode := arg.Copy()
	_, err := optimizeBooleanNodesDepth(ctx, newNode, 0, sharedIncludedNodes)
	return newNode, err
}

func optimizeBooleanNodesDepth(ctx expr.Includer, arg expr.Node, depth int, sharedIncludedNodes *SharedIncludedNodes) (uint64, error) {
	if depth > vm.MaxDepth {
		return 0, vm.ErrMaxDepth
	}
	result := uint64(1)
	switch n := arg.(type) {
	case *expr.BooleanNode:
		nodes := make([]nodeWithNumberOfChildren, len(n.Args))
		for i, narg := range n.Args {
			subTreeResult, err := optimizeBooleanNodesDepth(ctx, narg, depth+1, sharedIncludedNodes)
			if err != nil {
				return 0, err
			}
			result += subTreeResult
			nodes[i] = nodeWithNumberOfChildren{
				numberOfChildren: subTreeResult,
				node:             narg,
			}
		}
		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].numberOfChildren < nodes[j].numberOfChildren
		})
		for i, node := range nodes {
			n.Args[i] = node.node
		}
	case *expr.BinaryNode:
		for _, narg := range n.Args {
			subTreeResult, err := optimizeBooleanNodesDepth(ctx, narg, depth+1, sharedIncludedNodes)
			if err != nil {
				return 0, err
			}
			result += subTreeResult
		}
	case *expr.UnaryNode:
		subTreeResult, err := optimizeBooleanNodesDepth(ctx, n.Arg, depth+1, sharedIncludedNodes)
		if err != nil {
			return 0, err
		}
		result += subTreeResult
	case *expr.TriNode:
		for _, narg := range n.Args {
			subTreeResult, err := optimizeBooleanNodesDepth(ctx, narg, depth+1, sharedIncludedNodes)
			if err != nil {
				return 0, err
			}
			result += subTreeResult
		}
	case *expr.ArrayNode:
		for _, narg := range n.Args {
			subTreeResult, err := optimizeBooleanNodesDepth(ctx, narg, depth+1, sharedIncludedNodes)
			if err != nil {
				return 0, err
			}
			result += subTreeResult
		}
	case *expr.FuncNode:
		for _, narg := range n.Args {
			subTreeResult, err := optimizeBooleanNodesDepth(ctx, narg, depth+1, sharedIncludedNodes)
			if err != nil {
				return 0, err
			}
			result += subTreeResult
		}
	case *expr.IncludeNode:
		if sharedNode, ok := sharedIncludedNodes.data[n.Identity.Text]; ok {
			n.ExprNode = sharedNode.node
			result += sharedNode.numberOfChildren
		} else {
			incExpr, err := ctx.Include(n.Identity.Text)
			if err != nil {
				return 0, err
			}
			if incExpr == nil {
				return 0, expr.ErrIncludeNotFound
			}
			n.ExprNode = incExpr.Copy()
			subTreeResult, err := optimizeBooleanNodesDepth(ctx, n.ExprNode, depth+1, sharedIncludedNodes)
			if err != nil {
				return 0, err
			}
			sharedIncludedNodes.data[n.Identity.Text] = &nodeWithNumberOfChildren{
				numberOfChildren: subTreeResult,
				node:             n.ExprNode,
			}
			result += subTreeResult
		}
	}
	return result, nil
}
