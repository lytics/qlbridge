package lex

import (
	"testing"
	"time"

	u "github.com/araddon/gou"
	"github.com/stretchr/testify/assert"
)

var _ = u.EMPTY

func TestFilterDialectInit(t *testing.T) {
	// Make sure we can init more than once, see if it panics
	FilterQLDialect.Init()
	for _, stmt := range FilterQLDialect.Statements {
		assert.NotEqual(t, "", stmt.String())
	}
}

func verifyFilterQLTokens(t *testing.T, ql string, tokens []Token) {
	l := NewFilterQLLexer(ql)
	u.Debugf("filterql: %v", ql)
	for _, goodToken := range tokens {
		tok := l.NextToken()
		//u.Debugf("%#v  %#v", tok, goodToken)
		assert.Equal(t, tok.T, goodToken.T, "want='%v' has %v %v for %s", goodToken.T, tok.T, l.PeekX(10), l.RawInput())
		assert.Equal(t, tok.V, goodToken.V, "want='%v' has %v ", goodToken.V, tok.V)
	}
}

func TestFilterQLBasic(t *testing.T) {

	verifyFilterQLTokens(t, `
    FILTER AND (
          -- Lets make sure the date is good
          daysago(datefield) < 100
          -- as well as domain
          , domain(url) == "google.com"
          INCLUDE my_other_named_filter
          EXISTS my_field
          , OR (
              momentum > 20
             , propensity > 50
          )
          , NOT score > 20
       )
    ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenNewLine, ""),
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " Lets make sure the date is good"),
			tv(TokenNewLine, ""),
			tv(TokenUdfExpr, "daysago"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "datefield"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenLT, "<"),
			tv(TokenInteger, "100"),
			tv(TokenNewLine, ""),
			tv(TokenCommentSingleLine, "--"),
			tv(TokenComment, " as well as domain"),
			tv(TokenNewLine, ""),
			tv(TokenComma, ","),
			tv(TokenUdfExpr, "domain"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "url"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenEqualEqual, "=="),
			tv(TokenValue, "google.com"),
			tv(TokenNewLine, ""),
			tv(TokenInclude, "INCLUDE"),
			tv(TokenIdentity, "my_other_named_filter"),
			tv(TokenNewLine, ""),
			tv(TokenExists, "EXISTS"),
			tv(TokenIdentity, "my_field"),
			tv(TokenNewLine, ""),
			tv(TokenComma, ","),
			tv(TokenLogicOr, "OR"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenNewLine, ""),
			tv(TokenIdentity, "momentum"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenNewLine, ""),
			tv(TokenComma, ","),
			tv(TokenIdentity, "propensity"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "50"),
			tv(TokenNewLine, ""),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenComma, ","),
			tv(TokenNegate, "NOT"),
			tv(TokenIdentity, "score"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenNewLine, ""),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	verifyFilterQLTokens(t, `
    FILTER AND( score > 20 ) ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "score"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	verifyFilterQLTokens(t, `
    FILTER
      AND(score > 20)
    ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenNewLine, ""),
			tv(TokenLogicAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenIdentity, "score"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	// Ensure we support trailing commas
	verifyFilterQLTokens(t, `
    FILTER AND (
      	score > 20 ,
      )
    ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenNewLine, ""),
			tv(TokenIdentity, "score"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "20"),
			tv(TokenComma, ","),
			tv(TokenNewLine, ""),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	// Ensure we support new lines in
	verifyFilterQLTokens(t, `
    FILTER AND(
        score IN (20,
        30,
        60)
      )
    ALIAS my_filter_name
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenNewLine, ""),
			tv(TokenIdentity, "score"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "20"),
			tv(TokenComma, ","),
			tv(TokenInteger, "30"),
			tv(TokenComma, ","),
			tv(TokenInteger, "60"),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenRightParenthesis, ")"),
			tv(TokenNewLine, ""),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "my_filter_name"),
		})

	// Now for a really simple naked filter
	verifyFilterQLTokens(t, `
    FILTER x > 5
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "x"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "5"),
		})

	// With
	verifyFilterQLTokens(t, `
    FILTER x > 5
    WITH k = "stuff"
    ALIAS withstuff
    `,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "x"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "5"),
			tv(TokenNewLine, ""),
			tv(TokenWith, "WITH"),
			tv(TokenIdentity, "k"),
			tv(TokenEqual, "="),
			tv(TokenValue, "stuff"),
			tv(TokenAlias, "ALIAS"),
			tv(TokenIdentity, "withstuff"),
		})
}

func TestFilterQLIntersects(t *testing.T) {
	verifyFilterQLTokens(t, `FILTER score INTERSECTS (20, 30, 60)`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "score"),
			tv(TokenIntersects, "INTERSECTS"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "20"),
			tv(TokenComma, ","),
			tv(TokenInteger, "30"),
			tv(TokenComma, ","),
			tv(TokenInteger, "60"),
			tv(TokenRightParenthesis, ")"),
		})
}

// An unquoted negative numeric literal in a value position must lex as a
// single signed TokenInteger/TokenFloat, not a TokenMinus followed by a
// positive number.
func TestFilterQLNegativeLiteral(t *testing.T) {
	verifyFilterQLTokens(t, `FILTER visitct = -1`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenEqual, "="),
			tv(TokenInteger, "-1"),
		})

	verifyFilterQLTokens(t, `FILTER visitct = -1.5`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenEqual, "="),
			tv(TokenFloat, "-1.5"),
		})

	verifyFilterQLTokens(t, `FILTER visitct IN (-1)`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "-1"),
			tv(TokenRightParenthesis, ")"),
		})

	verifyFilterQLTokens(t, `FILTER visitct IN (-1, 3)`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "-1"),
			tv(TokenComma, ","),
			tv(TokenInteger, "3"),
			tv(TokenRightParenthesis, ")"),
		})

	verifyFilterQLTokens(t, `FILTER city IN (-1)`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "city"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "-1"),
			tv(TokenRightParenthesis, ")"),
		})
}

// A negative literal must not consume the clause continuation: everything
// after it (infix AND/OR, the rest of a list) still has to lex.
func TestFilterQLNegativeLiteralInfix(t *testing.T) {
	verifyFilterQLTokens(t, `FILTER visitct = -1 AND city = "sf"`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenEqual, "="),
			tv(TokenInteger, "-1"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenIdentity, "city"),
			tv(TokenEqual, "="),
			tv(TokenValue, "sf"),
		})

	verifyFilterQLTokens(t, `FILTER visitct = -1 OR city = "sf"`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenEqual, "="),
			tv(TokenInteger, "-1"),
			tv(TokenLogicOr, "OR"),
			tv(TokenIdentity, "city"),
			tv(TokenEqual, "="),
			tv(TokenValue, "sf"),
		})

	verifyFilterQLTokens(t, `FILTER visitct > -1 AND visitct < 5`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenGT, ">"),
			tv(TokenInteger, "-1"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenIdentity, "visitct"),
			tv(TokenLT, "<"),
			tv(TokenInteger, "5"),
		})

	verifyFilterQLTokens(t, `FILTER visitct BETWEEN 5 AND -1`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenBetween, "BETWEEN"),
			tv(TokenInteger, "5"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenInteger, "-1"),
		})

	// Sign directly after BETWEEN: the only shape where the previous token is
	// TokenBetween itself.
	verifyFilterQLTokens(t, `FILTER visitct BETWEEN -5 AND -1`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenBetween, "BETWEEN"),
			tv(TokenInteger, "-5"),
			tv(TokenLogicAnd, "AND"),
			tv(TokenInteger, "-1"),
		})
}

// A negative anywhere but first in a list: the `,` continuation must survive.
func TestFilterQLNegativeLiteralNotFirstInList(t *testing.T) {
	verifyFilterQLTokens(t, `FILTER visitct IN (1, -3)`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenInteger, "1"),
			tv(TokenComma, ","),
			tv(TokenInteger, "-3"),
			tv(TokenRightParenthesis, ")"),
		})

	verifyFilterQLTokens(t, `FILTER visitct IN ("a", -1)`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenIN, "IN"),
			tv(TokenLeftParenthesis, "("),
			tv(TokenValue, "a"),
			tv(TokenComma, ","),
			tv(TokenInteger, "-1"),
			tv(TokenRightParenthesis, ")"),
		})
}

// A sign the number scanner would reject must fall back to TokenMinus rather
// than commit to a literal LexNumber then hard-errors on.
func TestFilterQLSignedLiteralScannerDisagreement(t *testing.T) {
	verifyFilterQLTokens(t, `FILTER visitct = -.5`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenEqual, "="),
			tv(TokenMinus, "-"),
			tv(TokenIdentity, ".5"),
		})

	verifyFilterQLTokens(t, `FILTER visitct = -0x1A`,
		[]Token{
			tv(TokenFilter, "FILTER"),
			tv(TokenIdentity, "visitct"),
			tv(TokenEqual, "="),
			tv(TokenMinus, "-"),
			tv(TokenInteger, "0x1A"),
		})
}

// A trailing operator after a negative literal must terminate the scan.  An
// unbalanced state stack live-locks here instead, so the whole lex runs on a
// goroutine and the test fails on timeout rather than hanging the suite.
func TestFilterQLNegativeLiteralTrailingOperatorTerminates(t *testing.T) {
	for _, ql := range []string{`FILTER visitct = -1-`, `FILTER visitct = -1/`} {
		done := make(chan bool, 1)
		go func() {
			l := NewFilterQLLexer(ql)
			for i := 0; i < 100; i++ {
				tok := l.NextToken()
				if tok.T == TokenEOF || tok.T == TokenError {
					done <- true
					return
				}
			}
			done <- false
		}()

		select {
		case ok := <-done:
			assert.True(t, ok, "%s must reach EOF or Error within 100 tokens", ql)
		case <-time.After(10 * time.Second):
			t.Fatalf("%s did not terminate: lexer state stack is unbalanced", ql)
		}
	}
}
