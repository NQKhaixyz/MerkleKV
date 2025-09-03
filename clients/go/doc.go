/*
Package merklekv provides a Go client for MerkleKV distributed key-value store.

MerkleKV is a high-performance, distributed key-value store with self-healing
replication. This package provides a simple client interface for connecting
to MerkleKV servers and performing basic operations.

Basic usage:

	client := merklekv.New("localhost", 7878)
	err := client.Connect()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Set a key-value pair
	err = client.Set("user:123", "john_doe")
	if err != nil {
		log.Fatal(err)
	}

	// Get a value
	value, err := client.Get("user:123")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Value:", value) // Output: Value: john_doe

	// Delete a key
	err = client.Delete("user:123")
	if err != nil {
		log.Fatal(err)
	}

Context-aware operations with timeouts:

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	value, err := client.GetWithContext(ctx, "user:123")
	if err != nil {
		log.Fatal(err)
	}
*/
package merklekv
