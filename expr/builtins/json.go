package builtins

import (
	"encoding/json"
	"fmt"

	u "github.com/araddon/gou"
	"github.com/jmespath/go-jmespath"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/value"
)

var _ = u.EMPTY

// JsonPath jmespath json parser http://jmespath.org/
//
//	json_field = `[{"name":"n1","ct":8,"b":true, "tags":["a","b"]},{"name":"n2","ct":10,"b": false, "tags":["a","b"]}]`
//
//	json.jmespath(json_field, "[?name == 'n1'].name | [0]")  =>  "n1"
type JsonPath struct{}

func (m *JsonPath) Type() value.ValueType { return value.UnknownType }
func (m *JsonPath) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf(`Expected 2 args for json.jmespath(field,json_val) but got %s`, n)
	}

	jsonPathExpr := ""
	switch jn := n.Args[1].(type) {
	case *expr.StringNode:
		jsonPathExpr = jn.Text
	default:
		return nil, fmt.Errorf("expected a string expression for jmespath got %T", jn)
	}
	jmes, err := jmespath.Compile(jsonPathExpr)
	if err != nil {
		return nil, err
	}
	return jsonPathEval(jmes), nil
}

func jsonPathEval(jmes *jmespath.JMESPath) expr.EvaluatorFunc {
	return func(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
		if args[0] == nil || args[0].Err() || args[0].Nil() {
			return nil, false
		}
		a := args[0]
		var val []byte
		var err error
		switch {
		case a.Type().IsMap() || a.Type().IsSlice():
			// TODO (ajr): need to recursively change value.Value to interface{} and extract the values
			// this is a bit of a hack to do that
			val, err = json.Marshal(a.Value())
			if err != nil {
				return nil, false
			}
		default:
			val = []byte(args[0].ToString())
		}
		var data interface{}
		if err := json.Unmarshal(val, &data); err != nil {
			return nil, false
		}

		result, err := jmes.Search(data)
		if err != nil {
			return nil, false
		}
		return value.NewValue(result), true
	}
}
