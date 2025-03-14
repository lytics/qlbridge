package gentypes

import (
	"fmt"
)

type ErrorMissingField struct {
	err error
}

func (e *ErrorMissingField) Error() string {
	return e.err.Error()
}

// MissingFieldErrors are returned when a segment can't be evaluated due to a
// referenced field missing from a schema.
// MissingField creates a new MissingFieldError for the given field.
func MissingField(field string) error {
	return &ErrorMissingField{fmt.Errorf("missing field: %s", field)}
}
