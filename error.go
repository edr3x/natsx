package natsx

import (
	"errors"
	"fmt"
)

// TerminalError wraps an error to indicate it should not be retried.
// When a handler returns a TerminalError, the message will be terminated
// (removed from the queue) instead of being requeued for retry.
//
// Use this for unrecoverable errors such as:
//   - Message decode failures (corrupted payload)
//   - Validation errors (invalid data that will never succeed)
//   - Template rendering errors (bugs, not transient issues)
type TerminalError struct {
	Err error
}

func (e *TerminalError) Error() string {
	return e.Err.Error()
}

func (e *TerminalError) Unwrap() error {
	return e.Err
}

// NewTerminalError wraps an error to mark it as non-retryable.
func NewTerminalError(err error) error {
	if err == nil {
		return nil
	}
	return &TerminalError{Err: err}
}

// NewTerminalErrorf creates a new terminal error with a formatted message.
func NewTerminalErrorf(format string, args ...any) error {
	return &TerminalError{Err: fmt.Errorf(format, args...)}
}

// IsTerminalError checks if an error is a terminal error.
func IsTerminalError(err error) bool {
	var termErr *TerminalError
	return errors.As(err, &termErr)
}
