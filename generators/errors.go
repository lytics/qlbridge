package qlindex

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// ErrorTimedOut is returned when an Elasticsearch request is reported as
// having timed out by the coordinating node.
var ErrorTimedOut = errors.New("elasticsearch timed out")

// ErrorShardFailed is returned when 1 or more shards failed to respond to an
// otherwise successful request.
type ErrorShardFailed int

func NewErrorShardFailed(failures int) ErrorShardFailed { return ErrorShardFailed(failures) }

func (s ErrorShardFailed) Error() string {
	return fmt.Sprintf("%d shards failed", int(s))
}

// ErrorMalformedEntity is returned when an empty or malformed entity is
// encountered.
type ErrorMalformedEntity string

// ErrorResponse is returned when Elasticsearch responds with a non-2xx status
// code.
type ErrorResponse struct {
	Method string
	URL    string
	Status int    `json:"status"`
	Err    string `json:"error"`
}

// NewErrorResponse from an Elasticsearch response. Closing the
// underlying response body is up to the caller.
func NewErrorResponse(resp *http.Response) *ErrorResponse {
	r := ErrorResponse{
		Method: resp.Request.Method,
		URL:    resp.Request.URL.String(),
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		r.Err = err.Error()
		r.Status = resp.StatusCode
		return &r
	}
	return &r
}

func (r *ErrorResponse) Error() string {
	return fmt.Sprintf("Elasticsearch request %v %v return status=%v error=%v", r.Method, r.URL, r.Status, r.Err)
}
