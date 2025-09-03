package io.merklekv.client;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Asynchronous MerkleKV client for non-blocking interactions with MerkleKV server.
 * 
 * Example usage:
 * <pre>
 * AsyncMerkleKVClient client = new AsyncMerkleKVClient("localhost", 7878);
 * try {
 *     client.setAsync("key1", "value1")
 *         .thenCompose(v -> client.getAsync("key1"))
 *         .thenCompose(value -> {
 *             System.out.println("Value: " + value);
 *             return client.deleteAsync("key1");
 *         })
 *         .get(); // Block for completion
 * } finally {
 *     client.close();
 * }
 * </pre>
 */
public class AsyncMerkleKVClient implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(AsyncMerkleKVClient.class.getName());
    
    private final String host;
    private final int port;
    private final int timeoutMs;
    private final ExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    // Connection pool for async operations
    private final BlockingQueue<Socket> connectionPool = new LinkedBlockingQueue<>();
    private final int maxConnections;

    /**
     * Creates a new async MerkleKV client.
     * 
     * @param host the server host
     * @param port the server port
     * @throws MerkleKVException if initialization fails
     */
    public AsyncMerkleKVClient(String host, int port) throws MerkleKVException {
        this(host, port, 30000, 10); // 30 second timeout, 10 max connections
    }

    /**
     * Creates a new async MerkleKV client with custom settings.
     * 
     * @param host the server host
     * @param port the server port
     * @param timeoutMs connection timeout in milliseconds
     * @param maxConnections maximum number of connections in pool
     * @throws MerkleKVException if initialization fails
     */
    public AsyncMerkleKVClient(String host, int port, int timeoutMs, int maxConnections) throws MerkleKVException {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
        this.maxConnections = maxConnections;
        this.executor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "MerkleKV-Client-" + System.identityHashCode(this));
            t.setDaemon(true);
            return t;
        });
        
        // Pre-populate connection pool
        initializeConnectionPool();
    }

    /**
     * Initializes the connection pool with a few connections.
     */
    private void initializeConnectionPool() throws MerkleKVException {
        try {
            // Start with 2 connections
            for (int i = 0; i < Math.min(2, maxConnections); i++) {
                Socket socket = createConnection();
                connectionPool.offer(socket);
            }
            LOGGER.log(Level.INFO, "Initialized async MerkleKV client with connection pool");
        } catch (Exception e) {
            throw new ConnectionException("Failed to initialize connection pool", e);
        }
    }

    /**
     * Creates a new connection to the server.
     */
    private Socket createConnection() throws IOException {
        Socket socket = new Socket(host, port);
        socket.setSoTimeout(timeoutMs);
        return socket;
    }

    /**
     * Gets a connection from the pool or creates a new one.
     */
    private CompletableFuture<Socket> getConnection() {
        return CompletableFuture.supplyAsync(() -> {
            if (closed.get()) {
                throw new RuntimeException("Client is closed");
            }
            
            // Try to get from pool first
            Socket socket = connectionPool.poll();
            if (socket != null && !socket.isClosed()) {
                return socket;
            }
            
            // Create new connection if pool is empty or connection is closed
            try {
                return createConnection();
            } catch (IOException e) {
                throw new RuntimeException("Failed to create connection", e);
            }
        }, executor);
    }

    /**
     * Returns a connection to the pool.
     */
    private void returnConnection(Socket socket) {
        if (socket != null && !socket.isClosed() && connectionPool.size() < maxConnections) {
            connectionPool.offer(socket);
        } else if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing excess connection", e);
            }
        }
    }

    /**
     * Sends a command asynchronously and returns the response.
     */
    private CompletableFuture<String> sendCommandAsync(String command) {
        return getConnection()
            .thenCompose(socket -> {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        LOGGER.log(Level.FINE, "Sending async command: {0}", command);
                        
                        BufferedReader reader = new BufferedReader(new InputStreamReader(
                            socket.getInputStream(), StandardCharsets.UTF_8));
                        PrintWriter writer = new PrintWriter(new OutputStreamWriter(
                            socket.getOutputStream(), StandardCharsets.UTF_8), true);
                        
                        writer.println(command);
                        if (writer.checkError()) {
                            throw new RuntimeException("Failed to send command to server");
                        }
                        
                        String response = reader.readLine();
                        if (response == null) {
                            throw new RuntimeException("Server closed connection");
                        }
                        
                        LOGGER.log(Level.FINE, "Received async response: {0}", response);
                        returnConnection(socket);
                        return response;
                        
                    } catch (SocketTimeoutException e) {
                        try { socket.close(); } catch (IOException ex) {}
                        throw new RuntimeException("Operation timed out after " + timeoutMs + "ms", e);
                    } catch (IOException e) {
                        try { socket.close(); } catch (IOException ex) {}
                        throw new RuntimeException("I/O error during communication", e);
                    }
                }, executor);
            })
            .exceptionally(throwable -> {
                Throwable cause = throwable.getCause() != null ? throwable.getCause() : throwable;
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                throw new RuntimeException("Unexpected error", cause);
            });
    }

    /**
     * Gets a value by key from the MerkleKV store asynchronously.
     * 
     * @param key the key to retrieve
     * @return CompletableFuture containing the value associated with the key
     */
    public CompletableFuture<String> getAsync(String key) {
        if (key == null || key.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Key cannot be null or empty"));
        }
        
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is closed"));
        }
        
        String command = "GET " + key;
        return sendCommandAsync(command)
            .thenApply(response -> {
                if (response.equals("NOT_FOUND")) {
                    throw new RuntimeException(new KeyNotFoundException(key));
                } else if (response.startsWith("VALUE ")) {
                    return response.substring(6); // Remove "VALUE " prefix
                } else if (response.startsWith("ERROR ")) {
                    throw new RuntimeException(new ProtocolException("Server error: " + response.substring(6)));
                } else {
                    throw new RuntimeException(new ProtocolException("Unexpected response: " + response));
                }
            });
    }

    /**
     * Sets a key-value pair in the MerkleKV store asynchronously.
     * 
     * @param key the key to set
     * @param value the value to associate with the key
     * @return CompletableFuture that completes when the operation is done
     */
    public CompletableFuture<Void> setAsync(String key, String value) {
        if (key == null || key.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Key cannot be null or empty"));
        }
        if (value == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Value cannot be null"));
        }
        
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is closed"));
        }
        
        String command = "SET " + key + " " + value;
        return sendCommandAsync(command)
            .thenAccept(response -> {
                if (!response.equals("OK")) {
                    if (response.startsWith("ERROR ")) {
                        throw new RuntimeException(new ProtocolException("Server error: " + response.substring(6)));
                    } else {
                        throw new RuntimeException(new ProtocolException("Unexpected response: " + response));
                    }
                }
            });
    }

    /**
     * Deletes a key from the MerkleKV store asynchronously.
     * 
     * @param key the key to delete
     * @return CompletableFuture containing true if the key was deleted, false if it didn't exist
     */
    public CompletableFuture<Boolean> deleteAsync(String key) {
        if (key == null || key.isEmpty()) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Key cannot be null or empty"));
        }
        
        if (closed.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Client is closed"));
        }
        
        String command = "DELETE " + key;
        return sendCommandAsync(command)
            .thenApply(response -> {
                if (response.equals("OK")) {
                    return true;
                } else if (response.equals("NOT_FOUND")) {
                    return false;
                } else if (response.startsWith("ERROR ")) {
                    throw new RuntimeException(new ProtocolException("Server error: " + response.substring(6)));
                } else {
                    throw new RuntimeException(new ProtocolException("Unexpected response: " + response));
                }
            });
    }

    /**
     * Checks if the client is currently active.
     * 
     * @return true if active, false if closed
     */
    public boolean isActive() {
        return !closed.get();
    }

    /**
     * Gets the server host.
     * 
     * @return the server host
     */
    public String getHost() {
        return host;
    }

    /**
     * Gets the server port.
     * 
     * @return the server port
     */
    public int getPort() {
        return port;
    }

    /**
     * Gets the connection timeout in milliseconds.
     * 
     * @return the timeout in milliseconds
     */
    public int getTimeoutMs() {
        return timeoutMs;
    }

    /**
     * Gets the maximum number of connections in the pool.
     * 
     * @return the maximum connections
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Closes the client and all connections.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            LOGGER.log(Level.INFO, "Closing async MerkleKV client");
            
            // Close all connections in pool
            Socket socket;
            while ((socket = connectionPool.poll()) != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    LOGGER.log(Level.WARNING, "Error closing pooled connection", e);
                }
            }
            
            // Shutdown executor
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            
            LOGGER.log(Level.INFO, "Async MerkleKV client closed");
        }
    }
}
