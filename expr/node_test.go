package expr_test

import (
	"encoding/json"
	"reflect"
	"testing"

	u "github.com/araddon/gou"
	"github.com/lytics/qlbridge/lex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/value"
)

var pbTests = []string{
	`eq(event,"stuff") OR ge(party, 1)`,
	`"Portland" IN ("ohio")`,
	`"xyz" BETWEEN todate("1/1/2015") AND 50`,
	`name == "bob"`,
	`name = 'bob'`,
	`AND ( EXISTS x, EXISTS y )`,
	`AND ( EXISTS x, INCLUDE ref_name )`,
	`company = "Toys R"" Us"`,
	`providers.id != NULL`,
}

var copyTest = []string{
	`"Portland" IN ("ohio")`,
	`"xyz" NOT BETWEEN 10 AND 50`,
	`name == "bob"`,
	`name = 'bob'`,
	`AND ( EXISTS x, EXISTS y )`,
	`AND ( EXISTS x, INCLUDE ref_name )`,
	`NOT AND ( EXISTS x, INCLUDE ref_name, NOT OR (x > 5, y < 10) )`,
	`company = "Toys R"" Us"`,
	`NOT providers.id != NULL`,
}

func TestNodeCopy(t *testing.T) {
	t.Parallel()
	for _, exprText := range copyTest {
		exp, err := expr.ParseExpression(exprText)
		require.NoError(t, err)
		n2 := exp.Copy()
		assert.True(t, reflect.DeepEqual(exp, n2), "Expected Equal but got %v for %v", exp, n2)
		assert.True(t, exp.Equal(n2), "Expected Equal but got %v for %v", exp, n2)
	}
	// reflect.DeepEqual always returns false when comparing non-nil functions
	// So we do not use it here
	exprWithFuncs := `NOT OR (eq(event,"stuff"), ge(party, 1), todate("1/1/2015"), NOT INCLUDE ref_name)`
	exp, err := expr.ParseExpression(exprWithFuncs)
	require.NoError(t, err)
	n2 := exp.Copy()
	assert.True(t, exp.Equal(n2), "Expected Equal but got %v for %v", exp, n2)
	includeNode := &expr.IncludeNode{
		ExprNode: &expr.NullNode{},
		Identity: &expr.IdentityNode{
			Text: "ref_name",
		},
		Operator: lex.Token{},
	}
	includeNodeCopy := includeNode.Copy()
	require.IsType(t, &expr.IncludeNode{}, includeNodeCopy)
	assert.Nil(t, includeNodeCopy.(*expr.IncludeNode).ExprNode)
}

func TestExprRoundTrip(t *testing.T) {
	t.Parallel()
	for _, et := range exprTests {
		exp, err := expr.ParseExpression(et.qlText)
		if et.ok {
			assert.Equal(t, err, nil, "Should not error parse expr but got %v for %s", err, et.qlText)
			by, err := json.MarshalIndent(exp.Expr(), "", "  ")
			assert.Equal(t, err, nil)
			u.Debugf("%s", string(by))
			en := &expr.Expr{}
			err = json.Unmarshal(by, en)
			assert.Equal(t, err, nil)
			_, err = expr.NodeFromExpr(en)
			assert.Equal(t, err, nil, et.qlText)

			// by, _ = json.MarshalIndent(nn.Expr(), "", "  ")
			// u.Debugf("%s", string(by))

			// TODO: Fixme
			// u.Debugf("%s", nn)
			//assert.True(t, nn.Equal(exp), "%s  doesn't match %s", et.qlText, nn.String())

		} else {
			assert.NotEqual(t, nil, err)
		}

	}
}

func TestNodeJson(t *testing.T) {
	t.Parallel()
	for _, exprText := range pbTests {
		exp, err := expr.ParseExpression(exprText)
		assert.Equal(t, err, nil, "Should not error parse expr but got ", err, "for ", exprText)
		by, err := json.MarshalIndent(exp.Expr(), "", "  ")
		assert.Equal(t, err, nil)
		u.Debugf("%s", string(by))
	}
}

var _ = u.EMPTY

func TestIdentityNames(t *testing.T) {
	m := map[string]string{
		`count(visits)`:   "ct_visits",
		`x = y`:           "x",
		`x = y AND q = z`: "x",
		`min(year)`:       "min_year",
		`todate(year)`:    "year",
		`mapct(name)`:     "cts_name",
		`AND( year > 10)`: "year",
	}
	for expr_str, expected := range m {
		ex, err := expr.ParseExpression(expr_str)
		assert.Equal(t, nil, err)
		assert.Equal(t, expected, expr.FindIdentityName(0, ex, ""), "expected: %v for %v", expected, expr_str)
	}
	ne := expr.MustParse(`1 + "hello"`)
	n := expr.MustParse("eq(hello, world)")
	assert.Equal(t, "", expr.FindFirstIdentity(ne))
	assert.Equal(t, "hello", expr.FindFirstIdentity(n))
	assert.Equal(t, []string{"hello", "world"}, expr.FindAllLeftIdentityFields(n))
	assert.Equal(t, []string{"hello", "world"}, expr.FindAllIdentityField(n))

	assert.Equal(t, []string{"user.name", "world"}, expr.FindAllIdentityField(expr.MustParse("eq(user.name, world)")))

	assert.Equal(t, []string{"user", "world"}, expr.FindAllLeftIdentityFields(expr.MustParse("eq(user.name, world)")))

	assert.Equal(t, "", expr.FindFirstIdentity(expr.MustParse(`6 + toint("world")`)))
	assert.Equal(t, "name", expr.FindFirstIdentity(expr.MustParse(`AND (
		6 > 5
		toint(name)
	)`)))
	assert.Equal(t, "email", expr.FindFirstIdentity(expr.MustParse(`AND (
		NOT EXISTS email
		X between 4 and 5
	)`)))
	assert.Equal(t, "X", expr.FindFirstIdentity(expr.MustParse(`AND (
		X between 4 and 5
	)`)))
	assert.Equal(t, "Z", expr.FindFirstIdentity(expr.MustParse(`AND (
		"x" in (4,5,Z)
	)`)))
	assert.Equal(t, []string{"email", "name"}, expr.FilterSpecialIdentities([]string{"email", "name", "TRUE"}))
}

func TestValueTypeFromExpression(t *testing.T) {
	assert.Equal(t, value.UnknownType, expr.ValueTypeFromNode(expr.MustParse(`username`)))
	assert.Equal(t, value.StringType, expr.ValueTypeFromNode(expr.MustParse(`"hello"`)))
	assert.Equal(t, value.BoolType, expr.ValueTypeFromNode(expr.MustParse(`eq(a,b)`)))
	assert.Equal(t, value.NumberType, expr.ValueTypeFromNode(expr.MustParse(`12.2)`)))
	assert.Equal(t, value.UnknownType, expr.ValueTypeFromNode(nil))
	assert.Equal(t, value.BoolType, expr.ValueTypeFromNode(expr.MustParse(`x > y`)))
	assert.Equal(t, value.BoolType, expr.ValueTypeFromNode(expr.MustParse(`x AND y`)))
	assert.Equal(t, value.BoolType, expr.ValueTypeFromNode(expr.MustParse(`x > ( y * 7)`)))
	assert.Equal(t, value.BoolType, expr.ValueTypeFromNode(expr.MustParse(`x > ( y % 7)`)))
	assert.Equal(t, value.BoolType, expr.ValueTypeFromNode(expr.MustParse(`AND (x > y, z < 8)`)))
	assert.Equal(t, value.IntType, expr.ValueTypeFromNode(expr.MustParse(`y % 7`)))
	assert.Equal(t, value.NumberType, expr.ValueTypeFromNode(expr.MustParse(`y * 7`)))
}
