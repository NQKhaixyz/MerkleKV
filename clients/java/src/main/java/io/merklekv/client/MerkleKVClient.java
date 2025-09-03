package io.merklekv.client;

import java.io.*;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * Synchronous MerkleKV client for interacting with MerkleKV server.
 * 
 * Example usage:
 * <pre>
 * MerkleKVClient client = new MerkleKVClient("localhost", 7878);
 * try {
 *     client.set("key1", "value1");
 *     String value = client.get("key1");
 *     client.delete("key1");
 * } finally {
 *     client.close();
 * }
 * </pre>
 */
public class MerkleKVClient implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(MerkleKVClient.class.getName());
    
    private final String host;
    private final int port;
    private final int timeoutMs;
    private Socket socket;
    private BufferedReader reader;
    private PrintWriter writer;
    private boolean connected = false;

    /**
     * Creates a new MerkleKV client.
     * 
     * @param host the server host
     * @param port the server port
     * @throws MerkleKVException if connection fails
     */
    public MerkleKVClient(String host, int port) throws MerkleKVException {
        this(host, port, 30000); // 30 second default timeout
    }

    /**
     * Creates a new MerkleKV client with custom timeout.
     * 
     * @param host the server host
     * @param port the server port
     * @param timeoutMs connection timeout in milliseconds
     * @throws MerkleKVException if connection fails
     */
    public MerkleKVClient(String host, int port, int timeoutMs) throws MerkleKVException {
        this.host = host;
        this.port = port;
        this.timeoutMs = timeoutMs;
        connect();
    }

    /**
     * Establishes connection to the MerkleKV server.
     */
    private void connect() throws MerkleKVException {
        try {
            LOGGER.log(Level.FINE, "Connecting to MerkleKV server at {0}:{1}", new Object[]{host, port});
            
            socket = new Socket(host, port);
            socket.setSoTimeout(timeoutMs);
            
            reader = new BufferedReader(new InputStreamReader(
                socket.getInputStream(), StandardCharsets.UTF_8));
            writer = new PrintWriter(new OutputStreamWriter(
                socket.getOutputStream(), StandardCharsets.UTF_8), true);
            
            connected = true;
            LOGGER.log(Level.INFO, "Connected to MerkleKV server at {0}:{1}", new Object[]{host, port});
            
        } catch (IOException e) {
            throw new ConnectionException("Failed to connect to MerkleKV server at " + host + ":" + port, e);
        }
    }

    /**
     * Ensures the client is connected to the server.
     */
    private void ensureConnected() throws MerkleKVException {
        if (!connected || socket == null || socket.isClosed()) {
            connect();
        }
    }

    /**
     * Sends a command to the server and returns the response.
     */
    private String sendCommand(String command) throws MerkleKVException {
        ensureConnected();
        
        try {
            LOGGER.log(Level.FINE, "Sending command: {0}", command);
            writer.println(command);
            
            if (writer.checkError()) {
                throw new ConnectionException("Failed to send command to server");
            }
            
            String response = reader.readLine();
            if (response == null) {
                throw new ConnectionException("Server closed connection");
            }
            
            LOGGER.log(Level.FINE, "Received response: {0}", response);
            return response;
            
        } catch (SocketTimeoutException e) {
            throw new TimeoutException("Operation timed out after " + timeoutMs + "ms", e);
        } catch (IOException e) {
            connected = false;
            throw new ConnectionException("I/O error during communication", e);
        }
    }

    /**
     * Gets a value by key from the MerkleKV store.
     * 
     * @param key the key to retrieve
     * @return the value associated with the key
     * @throws MerkleKVException if the operation fails
     * @throws KeyNotFoundException if the key is not found
     */
    public String get(String key) throws MerkleKVException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        
        String command = "GET " + key;
        String response = sendCommand(command);
        
        if (response.equals("NOT_FOUND")) {
            throw new KeyNotFoundException(key);
        } else if (response.startsWith("VALUE ")) {
            return response.substring(6); // Remove "VALUE " prefix
        } else if (response.startsWith("ERROR ")) {
            throw new ProtocolException("Server error: " + response.substring(6));
        } else {
            throw new ProtocolException("Unexpected response: " + response);
        }
    }

    /**
     * Sets a key-value pair in the MerkleKV store.
     * 
     * @param key the key to set
     * @param value the value to associate with the key
     * @throws MerkleKVException if the operation fails
     */
    public void set(String key, String value) throws MerkleKVException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        if (value == null) {
            throw new IllegalArgumentException("Value cannot be null");
        }
        
        String command = "SET " + key + " " + value;
        String response = sendCommand(command);
        
        if (!response.equals("OK")) {
            if (response.startsWith("ERROR ")) {
                throw new ProtocolException("Server error: " + response.substring(6));
            } else {
                throw new ProtocolException("Unexpected response: " + response);
            }
        }
    }

    /**
     * Deletes a key from the MerkleKV store.
     * 
     * @param key the key to delete
     * @return true if the key was deleted, false if it didn't exist
     * @throws MerkleKVException if the operation fails
     */
    public boolean delete(String key) throws MerkleKVException {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Key cannot be null or empty");
        }
        
        String command = "DELETE " + key;
        String response = sendCommand(command);
        
        if (response.equals("OK")) {
            return true;
        } else if (response.equals("NOT_FOUND")) {
            return false;
        } else if (response.startsWith("ERROR ")) {
            throw new ProtocolException("Server error: " + response.substring(6));
        } else {
            throw new ProtocolException("Unexpected response: " + response);
        }
    }

    /**
     * Checks if the client is currently connected to the server.
     * 
     * @return true if connected, false otherwise
     */
    public boolean isConnected() {
        return connected && socket != null && !socket.isClosed();
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
     * Closes the connection to the server.
     */
    @Override
    public void close() {
        if (connected) {
            try {
                if (writer != null) {
                    writer.close();
                }
                if (reader != null) {
                    reader.close();
                }
                if (socket != null) {
                    socket.close();
                }
                LOGGER.log(Level.INFO, "Disconnected from MerkleKV server");
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Error closing connection", e);
            } finally {
                connected = false;
                socket = null;
                reader = null;
                writer = null;
            }
        }
    }
}
