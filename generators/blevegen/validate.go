package blevegen

import (
	"fmt"
	"strings"

	"github.com/araddon/gou"
	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/generators/gentypes"
	"github.com/lytics/qlbridge/lex"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
)

type TypeValidator struct {
	schema gentypes.SchemaColumns
}

func NewValidator(s gentypes.SchemaColumns) *TypeValidator {
	return &TypeValidator{schema: s}
}

func (m *TypeValidator) FilterValidate(stmt *rel.FilterStatement) error {
	return m.walkNode(stmt.Filter)
}

func (m *TypeValidator) walkNode(node expr.Node) error {
	switch n := node.(type) {
	case *expr.UnaryNode:
		return m.urnaryNode(n)
	case *expr.BooleanNode:
		return m.booleanNode(n)
	case *expr.BinaryNode:
		return m.binaryNode(n)
	case *expr.TriNode:
		return m.triNode(n)
	case *expr.IdentityNode:
		return m.identityNode(n)
	case *expr.IncludeNode:
		// We assume included statement has done its own validation
		return nil
	case *expr.FuncNode:
		return m.funcExpr(n)
	default:
		gou.Warnf("not handled type validation %v %T", node, node)
		return fmt.Errorf("blevegen: unsupported node in expression: %T (%s)", node, node)
	}
}

func (m *TypeValidator) identityNode(n *expr.IdentityNode) error {
	vt, ok := m.schema.Column(n.Text)
	if !ok {
		return gentypes.MissingField(n.OriginalText())
	}
	if vt == value.UnknownType {
		return fmt.Errorf("Unknown Field Type %s", n)
	}
	return nil
}

func (m *TypeValidator) urnaryNode(n *expr.UnaryNode) error {
	switch n.Operator.T {
	case lex.TokenExists:
		in, ok := n.Arg.(*expr.IdentityNode)
		if !ok {
			return fmt.Errorf("Expected Identity field %s got %T", n, n.Arg)
		}
		return m.identityNode(in)

	case lex.TokenNegate:
		// Validate that rhs = bool?
		return m.walkNode(n.Arg)
	}
	return nil
}

func (m *TypeValidator) booleanNode(bn *expr.BooleanNode) error {
	for _, arg := range bn.Args {
		err := m.walkNode(arg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *TypeValidator) binaryNode(node *expr.BinaryNode) error {
	// Type check binary expression arguments as they must be:
	// Identifier-Operator-Literal
	lhs, err := fieldValueType(m.schema, node.Args[0])
	if err != nil {
		return err
	}

	switch op := node.Operator.T; op {
	case lex.TokenGE, lex.TokenLE, lex.TokenGT, lex.TokenLT:
		// Bleve enforces that lhs, rhs must be same type
		switch lhs {
		case value.NumberType:
			// If left hand is number right hand needs to be number
		}
	case lex.TokenEqual, lex.TokenEqualEqual:
		// the VM supports both = and ==
	case lex.TokenNE:
		// ident(0) != literal(1)
	case lex.TokenContains:
		// ident CONTAINS literal
	case lex.TokenLike:
		// ident LIKE literal
	case lex.TokenIN, lex.TokenIntersects:
		// Build up list of arguments
	}

	return nil
}

func (m *TypeValidator) triNode(_ *expr.TriNode) error {
	return nil
}

func (m *TypeValidator) funcExpr(_ *expr.FuncNode) error {
	return nil
}

// This helper function is reused from the esgen package
func fieldValueType(s gentypes.SchemaColumns, n expr.Node) (value.ValueType, error) {
	ident, ok := n.(*expr.IdentityNode)
	if !ok {
		return value.UnknownType, fmt.Errorf("expected left-hand identity but found %s = %s", n.NodeType(), n)
	}

	// Try the direct column lookup
	vt, ok := s.Column(ident.Text)
	if ok {
		return vt, nil
	}

	if ident.HasLeftRight() {
		vt, ok := s.Column(ident.OriginalText())
		if ok {
			return vt, nil
		}
	}

	// Check if key is left.right
	parts := strings.SplitN(ident.Text, ".", 2)
	if len(parts) == 2 {
		// Nested field lookup
		vt, ok = s.Column(parts[0])
		if ok {
			return vt, nil
		}
	}

	return value.UnknownType, gentypes.MissingField(ident.OriginalText())
}

// Updated generator.go to add Bleve support
