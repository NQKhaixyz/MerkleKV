package io.merklekv.client.examples;

import io.merklekv.client.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Example demonstrating asynchronous MerkleKV client usage.
 */
public class AsyncExample {
    public static void main(String[] args) {
        try (AsyncMerkleKVClient client = new AsyncMerkleKVClient("localhost", 7878)) {
            System.out.println("Connected to MerkleKV server at localhost:7878");
            
            // Async operations with CompletableFuture
            CompletableFuture<Void> pipeline = client
                .setAsync("async:user:1", "Alice")
                .thenCompose(v -> {
                    System.out.println("Set async:user:1");
                    return client.setAsync("async:user:2", "Bob");
                })
                .thenCompose(v -> {
                    System.out.println("Set async:user:2");
                    return client.getAsync("async:user:1");
                })
                .thenCompose(value -> {
                    System.out.println("Retrieved async:user:1: " + value);
                    return client.getAsync("async:user:2");
                })
                .thenCompose(value -> {
                    System.out.println("Retrieved async:user:2: " + value);
                    return client.deleteAsync("async:user:1");
                })
                .thenCompose(deleted -> {
                    System.out.println("Deleted async:user:1: " + deleted);
                    return client.deleteAsync("nonexistent:key");
                })
                .thenApply(deleted -> {
                    System.out.println("Deleted nonexistent key: " + deleted);
                    return null;
                });
            
            // Wait for completion
            pipeline.get();
            
            // Parallel operations
            System.out.println("Running parallel operations...");
            CompletableFuture<Void> set1 = client.setAsync("parallel:1", "Value1");
            CompletableFuture<Void> set2 = client.setAsync("parallel:2", "Value2");
            CompletableFuture<Void> set3 = client.setAsync("parallel:3", "Value3");
            
            // Wait for all sets to complete
            CompletableFuture.allOf(set1, set2, set3).get();
            System.out.println("All parallel sets completed");
            
            // Parallel gets
            CompletableFuture<String> get1 = client.getAsync("parallel:1");
            CompletableFuture<String> get2 = client.getAsync("parallel:2");
            CompletableFuture<String> get3 = client.getAsync("parallel:3");
            
            CompletableFuture<Void> allGets = CompletableFuture.allOf(get1, get2, get3)
                .thenRun(() -> {
                    try {
                        System.out.println("Parallel results:");
                        System.out.println("  parallel:1 = " + get1.get());
                        System.out.println("  parallel:2 = " + get2.get());
                        System.out.println("  parallel:3 = " + get3.get());
                    } catch (InterruptedException | ExecutionException e) {
                        e.printStackTrace();
                    }
                });
            
            allGets.get();
            
            // Error handling example
            System.out.println("Testing error handling...");
            client.getAsync("nonexistent:key")
                .handle((result, throwable) -> {
                    if (throwable != null) {
                        System.out.println("Expected error caught: " + throwable.getCause().getClass().getSimpleName());
                    }
                    return null;
                })
                .get();
            
            // Unicode support
            client.setAsync("unicode:async", "异步测试 🚀")
                .thenCompose(v -> client.getAsync("unicode:async"))
                .thenAccept(value -> System.out.println("Unicode async value: " + value))
                .get();
            
            // Large value test
            StringBuilder largeValue = new StringBuilder();
            for (int i = 0; i < 500; i++) {
                largeValue.append("Async large value line ").append(i).append(". ");
            }
            
            client.setAsync("large:async", largeValue.toString())
                .thenCompose(v -> client.getAsync("large:async"))
                .thenAccept(value -> System.out.println("Large async value length: " + value.length()))
                .get();
            
            // Cleanup
            CompletableFuture<Boolean> cleanup1 = client.deleteAsync("async:user:2");
            CompletableFuture<Boolean> cleanup2 = client.deleteAsync("parallel:1");
            CompletableFuture<Boolean> cleanup3 = client.deleteAsync("parallel:2");
            CompletableFuture<Boolean> cleanup4 = client.deleteAsync("parallel:3");
            CompletableFuture<Boolean> cleanup5 = client.deleteAsync("unicode:async");
            CompletableFuture<Boolean> cleanup6 = client.deleteAsync("large:async");
            
            CompletableFuture.allOf(cleanup1, cleanup2, cleanup3, cleanup4, cleanup5, cleanup6).get();
            
            System.out.println("Async example completed successfully!");
            
        } catch (MerkleKVException e) {
            System.err.println("MerkleKV error: " + e.getMessage());
            e.printStackTrace();
        } catch (Exception e) {
            System.err.println("Unexpected error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
