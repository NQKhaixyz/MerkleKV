/**
 * Error classes for MerkleKV client.
 */

/**
 * Base error class for MerkleKV client errors.
 */
export class MerkleKVError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'MerkleKVError';
    }
}

/**
 * Thrown when connection to server fails.
 */
export class ConnectionError extends MerkleKVError {
    constructor(message: string) {
        super(message);
        this.name = 'ConnectionError';
    }
}

/**
 * Thrown when operation times out.
 */
export class TimeoutError extends MerkleKVError {
    constructor(message: string) {
        super(message);
        this.name = 'TimeoutError';
    }
}

/**
 * Thrown when server returns an error.
 */
export class ProtocolError extends MerkleKVError {
    constructor(message: string) {
        super(message);
        this.name = 'ProtocolError';
    }
}
