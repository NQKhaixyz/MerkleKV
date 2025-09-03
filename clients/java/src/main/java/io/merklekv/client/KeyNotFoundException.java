package io.merklekv.client;

/**
 * Exception thrown when a key is not found.
 */
public class KeyNotFoundException extends MerkleKVException {
    public KeyNotFoundException(String key) {
        super("Key not found: " + key);
    }
}
