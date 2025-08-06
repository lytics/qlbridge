package builtins

import (
	"fmt"
	"net"

	"github.com/lytics/qlbridge/expr"
	"github.com/lytics/qlbridge/value"
)

// IPFilter determines whether an IP address is contained within a given CIDR subnet
//
//	ipfilter("192.168.1.100", "192.168.1.0/24") => true
//	ipfilter("10.0.0.1", "192.168.1.0/24") => false
type IPFilter struct{}

// Type is Bool
func (m *IPFilter) Type() value.ValueType { return value.BoolType }

func (m *IPFilter) Validate(n *expr.FuncNode) (expr.EvaluatorFunc, error) {
	if len(n.Args) != 2 {
		return nil, fmt.Errorf("Expected 2 args for ipfilter(ip_address, subnet_cidr) but got %s", n)
	}
	return ipFilterEval, nil
}

func ipFilterEval(ctx expr.EvalContext, args []value.Value) (value.Value, bool) {
	// Convert arguments to strings
	ipStr, ipOk := value.ValueToString(args[0])
	cidrStr, cidrOk := value.ValueToString(args[1])

	if !ipOk || !cidrOk {
		return value.BoolValueFalse, false
	}

	// Parse the IP address
	ip := net.ParseIP(ipStr)
	if ip == nil {
		// Invalid IP address
		return value.BoolValueFalse, false
	}

	// Parse the CIDR notation
	_, ipNet, err := net.ParseCIDR(cidrStr)
	if err != nil {
		// Invalid CIDR notation
		return value.BoolValueFalse, false
	}

	// Check if IP is contained in the subnet
	if ipNet.Contains(ip) {
		return value.BoolValueTrue, true
	}

	return value.BoolValueFalse, true
}