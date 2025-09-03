/**
 * Error classes for MerkleKV client.
 */
/**
 * Base error class for MerkleKV client errors.
 */
export declare class MerkleKVError extends Error {
    constructor(message: string);
}
/**
 * Thrown when connection to server fails.
 */
export declare class ConnectionError extends MerkleKVError {
    constructor(message: string);
}
/**
 * Thrown when operation times out.
 */
export declare class TimeoutError extends MerkleKVError {
    constructor(message: string);
}
/**
 * Thrown when server returns an error.
 */
export declare class ProtocolError extends MerkleKVError {
    constructor(message: string);
}
