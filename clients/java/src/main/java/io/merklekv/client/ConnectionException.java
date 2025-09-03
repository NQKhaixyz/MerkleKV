package io.merklekv.client;

/**
 * Exception thrown when connection to server fails.
 */
public class ConnectionException extends MerkleKVException {
    public ConnectionException(String message) {
        super(message);
    }

    public ConnectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
