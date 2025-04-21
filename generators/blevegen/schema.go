package blevegen

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/generators/gentypes"
	"github.com/lytics/qlbridge/value"
)

// scalar returns a usable representation of a scalar node type for use in Bleve
// filters.
//
// Does not support Null.
func scalar(node expr.Node) (any, bool) {
	switch n := node.(type) {

	case *expr.StringNode:
		return n.Text, true

	case *expr.NumberNode:
		if n.IsInt {
			// Support ints
			return n.Int64, true
		}
		return n.Float64, true

	case *expr.ValueNode:
		// Make sure this is a scalar value node
		switch n.Value.Type() {
		case value.BoolType, value.IntType, value.StringType, value.TimeType:
			return n.String(), true
		case value.NumberType:
			nn, ok := n.Value.(floatval)
			if !ok {
				return nil, false
			}
			return nn.Float(), true
		}
	case *expr.IdentityNode:
		if _, err := strconv.ParseBool(n.Text); err == nil {
			return n.Text, true
		}

	}
	return "", false
}

func fieldType(s gentypes.SchemaColumns, n expr.Node) (*gentypes.FieldType, error) {
	ident, ok := n.(*expr.IdentityNode)
	if !ok {
		return nil, fmt.Errorf("expected left-hand identity but found %s = %s", n.NodeType(), n)
	}

	if s == nil {
		return nil, gentypes.MissingField(ident.Text)
	}
	// Try to get field info from schema
	ft, ok := s.ColumnInfo(ident.Text)
	if ok {
		ft.Field = ident.Text
		return ft, nil
	}

	if ident.HasLeftRight() {
		ft, ok := s.ColumnInfo(ident.OriginalText())
		if ok {
			return ft, nil
		}
	}

	// Check if key is left.right
	parts := strings.Index(ident.Text, ".")
	if parts != -1 {
		// Nested field lookup
		ft, ok = s.ColumnInfo(ident.Text[:parts])
		if ok {
			// FIXME (ajr) - this is a hack to get the nested field
			ft.Field = ident.OriginalText()
			return ft, nil
		}
	}

	return nil, gentypes.MissingField(ident.OriginalText())
}
