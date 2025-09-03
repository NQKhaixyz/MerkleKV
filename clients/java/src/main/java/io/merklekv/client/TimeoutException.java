package io.merklekv.client;

/**
 * Exception thrown when operation times out.
 */
public class TimeoutException extends MerkleKVException {
    public TimeoutException(String message) {
        super(message);
    }

    public TimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}
