package io.merklekv.client;

/**
 * Base exception class for all MerkleKV client errors.
 * This class and its subclasses are publicly accessible for external error handling.
 */
public class MerkleKVException extends Exception {
    public MerkleKVException(String message) {
        super(message);
    }

    public MerkleKVException(String message, Throwable cause) {
        super(message, cause);
    }
}
