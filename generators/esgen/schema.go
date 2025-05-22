package esgen

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/generators/gentypes"
	"github.com/lytics/qlbridge/value"
)

// scalar returns a JSONable representation of a scalar node type for use in ES
// filters.
//
// Does not support Null.
func scalar(node expr.Node) (any, bool) {
	switch n := node.(type) {

	case *expr.StringNode:
		return n.Text, true

	case *expr.NumberNode:
		if n.IsInt {
			// ES supports string encoded ints
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

	// TODO (erin pentecost): This shotgun approach sucks, see https://github.com/lytics/qlbridge/issues/159
	ft, ok := s.ColumnInfo(ident.Text)
	if ok {
		return ft, nil
	}

	//left, right, _ := expr.LeftRight(ident.Text)
	if ident.HasLeftRight() {
		ft, ok := s.ColumnInfo(ident.OriginalText())
		if ok {
			return ft, nil
		}
	}

	// This is legacy, we stupidly used to allow this:
	//
	//   `key_name.field value` -> "key_name", "field value"
	//
	// check if key is left.right
	idx := strings.Index(ident.Text, ".")
	if idx != -1 {
		// Nested field lookup
		ft, ok = s.ColumnInfo(ident.Text[:idx])
		if ok {
			return ft, nil
		}
	}

	return nil, gentypes.MissingField(ident.OriginalText())
}

func fieldValueType(s gentypes.SchemaColumns, n expr.Node) (value.ValueType, error) {

	ident, ok := n.(*expr.IdentityNode)
	if !ok {
		return value.UnknownType, fmt.Errorf("expected left-hand identity but found %s = %s", n.NodeType(), n)
	}

	// TODO: This shotgun approach sucks, see https://github.com/lytics/qlbridge/issues/159
	vt, ok := s.Column(ident.Text)
	if ok {
		return vt, nil
	}

	//left, right, _ := expr.LeftRight(ident.Text)
	if ident.HasLeftRight() {
		vt, ok := s.Column(ident.OriginalText())
		if ok {
			return vt, nil
		}
	}

	// This is legacy, we stupidly used to allow this:
	//
	//   `key_name.field value` -> "key_name", "field value"
	//
	// check if key is left.right
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
