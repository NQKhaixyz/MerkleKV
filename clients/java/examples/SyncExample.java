package io.merklekv.client.examples;

import io.merklekv.client.*;

/**
 * Example demonstrating synchronous MerkleKV client usage.
 */
public class SyncExample {
    public static void main(String[] args) {
        // Create client with default settings
        try (MerkleKVClient client = new MerkleKVClient("localhost", 7878)) {
            System.out.println("Connected to MerkleKV server at localhost:7878");
            
            // Set some key-value pairs
            System.out.println("Setting key-value pairs...");
            client.set("user:1:name", "Alice");
            client.set("user:1:email", "alice@example.com");
            client.set("user:2:name", "Bob");
            client.set("user:2:email", "bob@example.com");
            
            // Get values
            System.out.println("Getting values...");
            String name1 = client.get("user:1:name");
            String email1 = client.get("user:1:email");
            System.out.println("User 1: " + name1 + " (" + email1 + ")");
            
            String name2 = client.get("user:2:name");
            String email2 = client.get("user:2:email");
            System.out.println("User 2: " + name2 + " (" + email2 + ")");
            
            // Test Unicode support
            client.set("unicode:test", "测试值 🚀 Émoji");
            String unicodeValue = client.get("unicode:test");
            System.out.println("Unicode value: " + unicodeValue);
            
            // Delete operations
            System.out.println("Deleting keys...");
            boolean deleted1 = client.delete("user:1:name");
            boolean deleted2 = client.delete("nonexistent:key");
            System.out.println("Deleted user:1:name: " + deleted1);
            System.out.println("Deleted nonexistent key: " + deleted2);
            
            // Try to get deleted key
            try {
                client.get("user:1:name");
            } catch (KeyNotFoundException e) {
                System.out.println("Key not found as expected: " + e.getMessage());
            }
            
            // Test large value
            StringBuilder largeValue = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                largeValue.append("This is line ").append(i).append(" of a large value. ");
            }
            
            client.set("large:value", largeValue.toString());
            String retrievedLarge = client.get("large:value");
            System.out.println("Large value length: " + retrievedLarge.length());
            
            // Clean up
            client.delete("user:1:email");
            client.delete("user:2:name");
            client.delete("user:2:email");
            client.delete("unicode:test");
            client.delete("large:value");
            
            System.out.println("Sync example completed successfully!");
            
        } catch (MerkleKVException e) {
            System.err.println("MerkleKV error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
