package main

import (
	"context"
	"fmt"
	"log"
	"time"

	merklekv "github.com/AI-Decenter/MerkleKV/clients/go"
)

func main() {
	fmt.Println("=== MerkleKV Go Client Example ===")

	// Create client
	client := merklekv.New("localhost", 7878)

	// Connect to server
	err := client.Connect()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer client.Close()

	fmt.Println("Connected to MerkleKV server")

	// Basic operations example
	basicOperations(client)

	// Context operations example
	contextOperations(client)

	// Error handling example
	errorHandling(client)

	fmt.Println("Example completed successfully")
}

func basicOperations(client *merklekv.Client) {
	fmt.Println("\n--- Basic Operations ---")

	// Set some key-value pairs
	err := client.Set("user:123", "john_doe")
	if err != nil {
		log.Printf("Failed to set user:123: %v", err)
		return
	}

	err = client.Set("user:456", "jane_smith")
	if err != nil {
		log.Printf("Failed to set user:456: %v", err)
		return
	}

	err = client.Set("counter", "42")
	if err != nil {
		log.Printf("Failed to set counter: %v", err)
		return
	}

	// Get values
	user123, err := client.Get("user:123")
	if err != nil {
		log.Printf("Failed to get user:123: %v", err)
	} else {
		fmt.Printf("user:123 = %s\n", user123)
	}

	user456, err := client.Get("user:456")
	if err != nil {
		log.Printf("Failed to get user:456: %v", err)
	} else {
		fmt.Printf("user:456 = %s\n", user456)
	}

	counter, err := client.Get("counter")
	if err != nil {
		log.Printf("Failed to get counter: %v", err)
	} else {
		fmt.Printf("counter = %s\n", counter)
	}

	// Try to get non-existent key
	nonexistent, err := client.Get("does_not_exist")
	if err == merklekv.ErrNotFound {
		fmt.Println("non-existent key = <not found>")
	} else if err != nil {
		log.Printf("Error getting non-existent key: %v", err)
	} else {
		fmt.Printf("non-existent key = %s\n", nonexistent)
	}

	// Delete a key
	err = client.Delete("user:456")
	if err != nil {
		log.Printf("Failed to delete user:456: %v", err)
		return
	}

	deletedUser, err := client.Get("user:456")
	if err == merklekv.ErrNotFound {
		fmt.Println("user:456 after delete = <not found>")
	} else if err != nil {
		log.Printf("Error getting deleted key: %v", err)
	} else {
		fmt.Printf("user:456 after delete = %s\n", deletedUser)
	}

	// Test empty value
	err = client.Set("empty_key", "")
	if err != nil {
		log.Printf("Failed to set empty value: %v", err)
		return
	}

	emptyValue, err := client.Get("empty_key")
	if err != nil {
		log.Printf("Failed to get empty value: %v", err)
	} else {
		fmt.Printf("empty_key = \"%s\"\n", emptyValue)
	}

	// Test Unicode
	err = client.Set("unicode_key", "Hello 世界 🚀")
	if err != nil {
		log.Printf("Failed to set unicode value: %v", err)
		return
	}

	unicodeValue, err := client.Get("unicode_key")
	if err != nil {
		log.Printf("Failed to get unicode value: %v", err)
	} else {
		fmt.Printf("unicode_key = %s\n", unicodeValue)
	}
}

func contextOperations(client *merklekv.Client) {
	fmt.Println("\n--- Context Operations ---")

	// Use context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Set with context
	err := client.SetWithContext(ctx, "ctx:key", "ctx_value")
	if err != nil {
		log.Printf("Failed to set with context: %v", err)
		return
	}

	// Get with context
	value, err := client.GetWithContext(ctx, "ctx:key")
	if err != nil {
		log.Printf("Failed to get with context: %v", err)
	} else {
		fmt.Printf("ctx:key = %s\n", value)
	}

	// Delete with context
	err = client.DeleteWithContext(ctx, "ctx:key")
	if err != nil {
		log.Printf("Failed to delete with context: %v", err)
		return
	}

	// Ping with context
	err = client.PingWithContext(ctx)
	if err != nil {
		log.Printf("Failed to ping with context: %v", err)
	} else {
		fmt.Println("Ping successful")
	}
}

func errorHandling(client *merklekv.Client) {
	fmt.Println("\n--- Error Handling ---")

	// Test empty key error
	_, err := client.Get("")
	if err == merklekv.ErrEmptyKey {
		fmt.Println("Empty key error handled correctly")
	} else {
		log.Printf("Unexpected error for empty key: %v", err)
	}

	// Test not found error
	_, err = client.Get("definitely_does_not_exist")
	if err == merklekv.ErrNotFound {
		fmt.Println("Not found error handled correctly")
	} else {
		log.Printf("Unexpected error for not found: %v", err)
	}

	// Test ping (should work)
	err = client.Ping()
	if err != nil {
		log.Printf("Ping failed: %v", err)
	} else {
		fmt.Println("Ping successful")
	}
}
