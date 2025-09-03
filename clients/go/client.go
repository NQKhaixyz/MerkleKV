package merklekv

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

// Client represents a connection to a MerkleKV server.
type Client struct {
	host    string
	port    int
	timeout time.Duration

	// Connection state
	mu       sync.RWMutex
	conn     net.Conn
	reader   *bufio.Reader
	writer   *bufio.Writer
	connected bool
}

// New creates a new MerkleKV client.
//
// Parameters:
//   - host: Server hostname (e.g., "localhost")
//   - port: Server port (e.g., 7878)
//
// Returns a new Client instance. Call Connect() to establish connection.
func New(host string, port int) *Client {
	return &Client{
		host:    host,
		port:    port,
		timeout: 5 * time.Second,
	}
}

// NewWithTimeout creates a new MerkleKV client with custom timeout.
//
// Parameters:
//   - host: Server hostname (e.g., "localhost")
//   - port: Server port (e.g., 7878)
//   - timeout: Connection and operation timeout
//
// Returns a new Client instance with custom timeout.
func NewWithTimeout(host string, port int, timeout time.Duration) *Client {
	return &Client{
		host:    host,
		port:    port,
		timeout: timeout,
	}
}

// Connect establishes a connection to the MerkleKV server.
//
// Returns an error if connection fails.
func (c *Client) Connect() error {
	return c.ConnectWithContext(context.Background())
}

// ConnectWithContext establishes a connection to the MerkleKV server with context.
//
// The context can be used to cancel the connection attempt or set a timeout.
//
// Returns an error if connection fails or context is canceled.
func (c *Client) ConnectWithContext(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected && c.conn != nil {
		return nil // Already connected
	}

	// Create a dialer with timeout
	dialer := &net.Dialer{
		Timeout: c.timeout,
	}

	address := fmt.Sprintf("%s:%d", c.host, c.port)
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return &ConnectionError{Op: "connect", Err: err}
	}

	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.writer = bufio.NewWriter(conn)
	c.connected = true

	return nil
}

// Close closes the connection to the server.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil {
		err := c.conn.Close()
		c.conn = nil
		c.reader = nil
		c.writer = nil
		c.connected = false
		return err
	}

	c.connected = false
	return nil
}

// IsConnected returns true if the client is connected to the server.
func (c *Client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected && c.conn != nil
}

// sendCommand sends a command to the server and returns the response.
func (c *Client) sendCommand(ctx context.Context, command string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.connected || c.conn == nil {
		return "", ErrNotConnected
	}

	// Set deadline based on context or timeout
	deadline, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		deadline = time.Now().Add(c.timeout)
	}

	err := c.conn.SetDeadline(deadline)
	if err != nil {
		return "", &ConnectionError{Op: "set deadline", Err: err}
	}

	// Send command with CRLF termination
	_, err = c.writer.WriteString(command + "\r\n")
	if err != nil {
		c.connected = false
		return "", &ConnectionError{Op: "write", Err: err}
	}

	err = c.writer.Flush()
	if err != nil {
		c.connected = false
		return "", &ConnectionError{Op: "flush", Err: err}
	}

	// Read response
	response, err := c.reader.ReadString('\n')
	if err != nil {
		c.connected = false
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return "", &TimeoutError{Op: "read response", Timeout: c.timeout.String()}
		}
		return "", &ConnectionError{Op: "read", Err: err}
	}

	// Clean up response (remove \r\n)
	response = strings.TrimSpace(response)

	// Check for protocol errors
	if strings.HasPrefix(response, "ERROR ") {
		errorMsg := strings.TrimPrefix(response, "ERROR ")
		return "", &ProtocolError{Op: "command", Message: errorMsg}
	}

	return response, nil
}

// formatSetValue formats a value for the SET command according to protocol rules.
// Empty values are represented as "" in the SET command.
func formatSetValue(value string) string {
	if value == "" {
		return `""`
	}
	return value
}

// Get retrieves the value for a key.
//
// Returns the value if the key exists, or ErrNotFound if the key doesn't exist.
func (c *Client) Get(key string) (string, error) {
	return c.GetWithContext(context.Background(), key)
}

// GetWithContext retrieves the value for a key with context support.
//
// The context can be used to cancel the operation or set a timeout.
//
// Returns the value if the key exists, or ErrNotFound if the key doesn't exist.
func (c *Client) GetWithContext(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", ErrEmptyKey
	}

	response, err := c.sendCommand(ctx, fmt.Sprintf("GET %s", key))
	if err != nil {
		return "", err
	}

	if response == "NOT_FOUND" {
		return "", ErrNotFound
	}

	if strings.HasPrefix(response, "VALUE ") {
		return strings.TrimPrefix(response, "VALUE "), nil
	}

	return "", &ProtocolError{Op: "get", Message: fmt.Sprintf("unexpected response: %s", response)}
}

// Set stores a key-value pair.
//
// Returns an error if the operation fails.
func (c *Client) Set(key, value string) error {
	return c.SetWithContext(context.Background(), key, value)
}

// SetWithContext stores a key-value pair with context support.
//
// The context can be used to cancel the operation or set a timeout.
//
// Returns an error if the operation fails.
func (c *Client) SetWithContext(ctx context.Context, key, value string) error {
	if key == "" {
		return ErrEmptyKey
	}

	// Format the command using the helper function
	command := fmt.Sprintf("SET %s %s", key, formatSetValue(value))

	response, err := c.sendCommand(ctx, command)
	if err != nil {
		return err
	}

	if response != "OK" {
		return &ProtocolError{Op: "set", Message: fmt.Sprintf("unexpected response: %s", response)}
	}

	return nil
}

// Delete removes a key from the store.
//
// Returns an error if the operation fails. Note that deleting a non-existent
// key is not considered an error and will return nil.
func (c *Client) Delete(key string) error {
	return c.DeleteWithContext(context.Background(), key)
}

// DeleteWithContext removes a key from the store with context support.
//
// The context can be used to cancel the operation or set a timeout.
//
// Returns an error if the operation fails. Note that deleting a non-existent
// key is not considered an error and will return nil.
func (c *Client) DeleteWithContext(ctx context.Context, key string) error {
	if key == "" {
		return ErrEmptyKey
	}

	response, err := c.sendCommand(ctx, fmt.Sprintf("DELETE %s", key))
	if err != nil {
		return err
	}

	if response != "OK" {
		return &ProtocolError{Op: "delete", Message: fmt.Sprintf("unexpected response: %s", response)}
	}

	return nil
}

// Ping sends a ping command to test connectivity.
//
// This is useful for health checks and ensuring the connection is still alive.
//
// Returns an error if the ping fails.
func (c *Client) Ping() error {
	return c.PingWithContext(context.Background())
}

// PingWithContext sends a ping command to test connectivity with context support.
//
// The context can be used to cancel the operation or set a timeout.
//
// Returns an error if the ping fails.
func (c *Client) PingWithContext(ctx context.Context) error {
	response, err := c.sendCommand(ctx, "PING")
	if err != nil {
		return err
	}

	if response != "PONG" && response != "OK" {
		return &ProtocolError{Op: "ping", Message: fmt.Sprintf("unexpected response: %s", response)}
	}

	return nil
}
