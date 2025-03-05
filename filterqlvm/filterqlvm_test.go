package filterqlvm

import (
	"fmt"
	"testing"
	"time"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/rel"
	"github.com/lytics/qlbridge/value"
	"github.com/lytics/qlbridge/vm"
)

// testContext implements expr.EvalContext for testing
type testContext struct {
	data map[string]value.Value
}

func (tc *testContext) Get(key string) (value.Value, bool) {
	v, ok := tc.data[key]
	left, right, hasNamespacing := expr.LeftRight(key)

	//u.Debugf("left:%q right:%q    key=%v", left, right, key)
	if hasNamespacing {
		f, ok := tc.data[left]
		if !ok {
			return nil, false
		}
		mapf, ok := f.(value.Map)
		if !ok {
			return nil, false
		}
		v, ok := mapf.Get(right)
		return v, ok
	}

	if f, ok := tc.data[right]; ok {
		return f, true
	}

	return v, ok
}

func (tc *testContext) Row() map[string]value.Value {
	return tc.data
}

func (tc *testContext) Ts() time.Time {
	return time.Now()
}

// Basic context with simple string and number fields
func newBasicContext() *testContext {
	return &testContext{
		data: map[string]value.Value{
			"name":     value.NewStringValue("John Doe"),
			"age":      value.NewIntValue(30),
			"score":    value.NewNumberValue(92.5),
			"active":   value.NewBoolValue(true),
			"created":  value.NewTimeValue(time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)),
			"tags":     value.NewStringsValue([]string{"developer", "golang", "database"}),
			"metadata": value.NewMapValue(map[string]interface{}{"level": 5, "type": "user"}),
		},
	}
}

// Complex context with nested data and multiple value types
func newComplexContext() *testContext {
	tags := value.NewStringsValue([]string{
		"mobile", "web", "desktop", "backend", "frontend",
		"developer", "tester", "designer", "manager", "admin",
	})

	metrics := make(map[string]interface{})
	for i := 0; i < 20; i++ {
		metrics[fmt.Sprintf("metric_%d", i)] = float64(i * 10)
	}

	return &testContext{
		data: map[string]value.Value{
			"user_id":      value.NewIntValue(12345),
			"username":     value.NewStringValue("johndoe"),
			"email":        value.NewStringValue("john.doe@example.com"),
			"first_name":   value.NewStringValue("John"),
			"last_name":    value.NewStringValue("Doe"),
			"age":          value.NewIntValue(35),
			"score":        value.NewNumberValue(92.5),
			"active":       value.NewBoolValue(true),
			"verified":     value.NewBoolValue(true),
			"premium":      value.NewBoolValue(false),
			"created_at":   value.NewTimeValue(time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)),
			"updated_at":   value.NewTimeValue(time.Date(2023, 6, 10, 0, 0, 0, 0, time.UTC)),
			"last_login":   value.NewTimeValue(time.Now().Add(-24 * time.Hour)),
			"login_count":  value.NewIntValue(150),
			"tags":         tags,
			"roles":        value.NewStringsValue([]string{"user", "editor"}),
			"preferences":  value.NewMapValue(map[string]interface{}{"theme": "dark", "notifications": true, "language": "en"}),
			"address":      value.NewMapValue(map[string]interface{}{"city": "New York", "country": "USA", "zip": "10001"}),
			"metrics":      value.NewMapValue(metrics),
			"subscription": value.NewMapValue(map[string]interface{}{"plan": "pro", "amount": 99.99, "currency": "USD", "active": true}),
			"devices": value.NewSliceValues([]value.Value{
				value.NewMapValue(map[string]interface{}{"type": "mobile", "os": "iOS", "last_used": time.Now().Add(-2 * 24 * time.Hour)}),
				value.NewMapValue(map[string]interface{}{"type": "desktop", "os": "Windows", "last_used": time.Now().Add(-12 * time.Hour)}),
			}),
		},
	}
}

// Define benchmark patterns with varying complexity
var benchmarkPatterns = []struct {
	name    string
	filter  string
	complex bool // whether to use complex context
}{
	{"Simple equality", `name = "John Doe"`, false},
	{"Numeric comparison", `age > 25`, false},
	{"String LIKE", `name LIKE "John%"`, false},
	{"IN operator", `"golang" IN tags`, false},
	{"Simple AND", `name = "John Doe" AND age > 25`, false},
	{"Simple OR", `score > 95 OR active = true`, false},
	{"Nested logic", `(age > 20 OR score > 95) AND active = true`, false},

	// More complex patterns for complex context
	{"Complex string comparison", `first_name = "John" AND last_name = "Doe"`, true},
	{"Multiple numeric comparisons", `age >= 30 AND score > 90 AND login_count > 100`, true},
	{"Map field access", `preferences.theme = "dark"`, true},
	{"Multiple IN checks", `AND ("user" IN rolesm, "admin" NOT IN roles)`, true},
	{"Complex AND OR", `AND (OR (premium = false, score > 90), AND(verified = true, active = true))`, true},
	{"Date comparisons", `created_at < updated_at AND last_login > created_at`, true},
	{"Complex string operations", `AND (email LIKE "%@example.com" , username LIKE "john%" )`, true},
	{"Deep nested conditions", `OR (AND (age > 30, score > 90) , AND (login_count > 100 , OR (premium = true, active = true)))`, true},
	{"Field exists checks", `AND (EXISTS subscription , subscription.active = true)`, true},
	{"Mathematical operations", `AND(age * 2 > 50 , score / 10 > 9)`, true},

	// Very complex pattern combining many operators
	{"Very complex pattern",
		`AND (
			AND (first_name = "John", last_name = "Doe"),
		 	OR (age > 30, score >= 90) ,
		 	OR ("user" IN roles, "admin" IN roles),
		 	OR (preferences.theme = "dark", preferences.language = "en"),
		 	(created_at < updated_at),
		 	AND (subscription.plan = "pro", subscription.amount < 100),
		 	(email LIKE "%@example.com")
		)`,
		true},
}

// Benchmarks for Standard VM
func BenchmarkStandardVM(b *testing.B) {
	for _, pattern := range benchmarkPatterns {
		b.Run(pattern.name, func(b *testing.B) {
			filter, err := rel.ParseFilterQL("FILTER " + pattern.filter)
			if err != nil {
				b.Fatalf("Failed to parse filter: %v", err)
			}

			var ctx *testContext
			if pattern.complex {
				ctx = newComplexContext()
			} else {
				ctx = newBasicContext()
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				vm.Matches(ctx, filter)
			}
		})
	}
}

// Benchmarks for Optimized VM
func BenchmarkOptimizedVM(b *testing.B) {
	optimizedVM := NewOptimizedVM()

	for _, pattern := range benchmarkPatterns {
		b.Run(pattern.name, func(b *testing.B) {
			filter, err := rel.ParseFilterQL("FILTER " + pattern.filter)
			if err != nil {
				b.Fatalf("Failed to parse filter: %v", err)
			}

			var ctx *testContext
			if pattern.complex {
				ctx = newComplexContext()
			} else {
				ctx = newBasicContext()
			}

			// Pre-compile to ensure we're measuring execution time, not compilation
			_, err = optimizedVM.CompileFilter(filter)
			if err != nil {
				b.Fatalf("Failed to compile filter: %v", err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				optimizedVM.Matches(ctx, filter)
			}
		})
	}
}

// Verify that both VMs produce consistent results
func TestVMConsistency(t *testing.T) {
	standardVM := &FilterQLVM{} // Wrapper for the standard VM
	optimizedVM := NewOptimizedVM()

	for _, pattern := range benchmarkPatterns {
		t.Run(pattern.name, func(t *testing.T) {
			filter, err := rel.ParseFilterQL("FILTER " + pattern.filter)
			if err != nil {
				t.Fatalf("Failed to parse filter: %v", err)
			}

			var ctx *testContext
			if pattern.complex {
				ctx = newComplexContext()
			} else {
				ctx = newBasicContext()
			}

			// Run with standard VM
			standardMatches, standardOk := standardVM.Matches(ctx, filter)

			// Run with optimized VM
			optimizedMatches, optimizedOk := optimizedVM.Matches(ctx, filter)

			// Compare results
			if standardOk != optimizedOk {
				t.Errorf("VM ok status differs: standard=%v, optimized=%v", standardOk, optimizedOk)
			}

			if standardMatches != optimizedMatches {
				t.Errorf("VM match results differ: standard=%v, optimized=%v", standardMatches, optimizedMatches)
			}
		})
	}
}

// FilterQLVM is a wrapper for the standard VM
type FilterQLVM struct{}

func (ovm *FilterQLVM) Matches(ctx expr.EvalContext, stmt *rel.FilterStatement) (bool, bool) {
	return vm.Matches(ctx, stmt)
}

func (vm *FilterQLVM) MatchesInc(inc expr.Includer, cr expr.EvalContext, stmt *rel.FilterStatement) (bool, bool) {
	return vm.MatchesInc(inc, cr, stmt)
}
