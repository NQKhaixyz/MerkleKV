package merklekv

import (
	"errors"
	"fmt"
)

// Error types for MerkleKV client operations.
var (
	// ErrNotConnected is returned when attempting to perform operations on a disconnected client.
	ErrNotConnected = errors.New("not connected to server")

	// ErrEmptyKey is returned when an empty key is provided to an operation.
	ErrEmptyKey = errors.New("key cannot be empty")

	// ErrNotFound is returned when a key is not found in the store.
	ErrNotFound = errors.New("key not found")
)

// ConnectionError represents connection-related errors.
type ConnectionError struct {
	Op  string
	Err error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection error during %s: %v", e.Op, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// TimeoutError represents timeout-related errors.
type TimeoutError struct {
	Op      string
	Timeout string
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("timeout during %s after %s", e.Op, e.Timeout)
}

// ProtocolError represents protocol-related errors from the server.
type ProtocolError struct {
	Op      string
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("protocol error during %s: %s", e.Op, e.Message)
}
