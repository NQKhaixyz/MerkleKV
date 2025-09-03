package io.merklekv.client;

/**
 * Exception thrown when server returns an error.
 */
public class ProtocolException extends MerkleKVException {
    public ProtocolException(String message) {
        super(message);
    }

    public ProtocolException(String message, Throwable cause) {
        super(message, cause);
    }
}
