package gentypes

import (
	"strconv"
	"strings"
)

func StringToLatLng(s string) (float64, float64, bool) {
	if len(s) < 1 {
		return 0, 0, false
	}
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return 0, 0, false
	}
	first, err := strconv.ParseFloat(strings.TrimSpace(parts[0]), 64)
	if err != nil {
		return 0, 0, false
	}
	second, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
	if err != nil {
		return 0, 0, false
	}
	return first, second, true
}
