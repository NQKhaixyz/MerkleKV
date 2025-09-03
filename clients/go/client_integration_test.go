//go:build integration

package merklekv

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests (these require a running MerkleKV server)
// Run with: go test -tags=integration

func TestIntegrationBasicOperations(t *testing.T) {
	client := New("localhost", 7878)
	client.timeout = 10 * time.Second // Longer timeout for CI
	
	err := client.Connect()
	require.NoError(t, err)
	defer client.Close()
	
	assert.True(t, client.IsConnected())
	
	// Test SET and GET
	err = client.Set("test_key", "test_value")
	require.NoError(t, err)
	
	value, err := client.Get("test_key")
	require.NoError(t, err)
	assert.Equal(t, "test_value", value)
	
	// Test GET non-existent key
	_, err = client.Get("nonexistent_key")
	assert.Equal(t, ErrNotFound, err)
	
	// Test DELETE
	err = client.Delete("test_key")
	require.NoError(t, err)
	
	_, err = client.Get("test_key")
	assert.Equal(t, ErrNotFound, err)
	
	// Test empty value
	err = client.Set("empty_key", "")
	require.NoError(t, err)
	
	value, err = client.Get("empty_key")
	require.NoError(t, err)
	assert.Equal(t, `""`, value) // Server quotes empty values
}

func TestIntegrationContextOperations(t *testing.T) {
	client := New("localhost", 7878)
	
	ctx := context.Background()
	err := client.ConnectWithContext(ctx)
	require.NoError(t, err)
	defer client.Close()
	
	// Test context operations
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	err = client.SetWithContext(ctx, "ctx_key", "ctx_value")
	require.NoError(t, err)
	
	value, err := client.GetWithContext(ctx, "ctx_key")
	require.NoError(t, err)
	assert.Equal(t, "ctx_value", value)
	
	err = client.DeleteWithContext(ctx, "ctx_key")
	require.NoError(t, err)
}

func TestIntegrationPing(t *testing.T) {
	client := New("localhost", 7878)
	
	err := client.Connect()
	require.NoError(t, err)
	defer client.Close()
	
	err = client.Ping()
	assert.NoError(t, err)
	
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	err = client.PingWithContext(ctx)
	assert.NoError(t, err)
}

func TestIntegrationLargeValue(t *testing.T) {
	client := New("localhost", 7878)
	client.timeout = 15 * time.Second // Longer timeout for large data
	
	err := client.Connect()
	require.NoError(t, err)
	defer client.Close()
	
	// Test with 10KB value
	largeValue := make([]byte, 10240)
	for i := range largeValue {
		largeValue[i] = 'x'
	}
	largeValueStr := string(largeValue)
	
	err = client.Set("large_key", largeValueStr)
	require.NoError(t, err)
	
	retrievedValue, err := client.Get("large_key")
	require.NoError(t, err)
	assert.Equal(t, len(largeValueStr), len(retrievedValue))
}

func TestIntegrationUnicode(t *testing.T) {
	client := New("localhost", 7878)
	
	err := client.Connect()
	require.NoError(t, err)
	defer client.Close()
	
	// Test Unicode handling
	unicodeKey := "测试key"
	unicodeValue := "测试value 🚀"
	
	err = client.Set(unicodeKey, unicodeValue)
	require.NoError(t, err)
	
	retrievedValue, err := client.Get(unicodeKey)
	require.NoError(t, err)
	assert.Equal(t, unicodeValue, retrievedValue)
}
