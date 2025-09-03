"use strict";
/**
 * Error classes for MerkleKV client.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.ProtocolError = exports.TimeoutError = exports.ConnectionError = exports.MerkleKVError = void 0;
/**
 * Base error class for MerkleKV client errors.
 */
class MerkleKVError extends Error {
    constructor(message) {
        super(message);
        this.name = 'MerkleKVError';
    }
}
exports.MerkleKVError = MerkleKVError;
/**
 * Thrown when connection to server fails.
 */
class ConnectionError extends MerkleKVError {
    constructor(message) {
        super(message);
        this.name = 'ConnectionError';
    }
}
exports.ConnectionError = ConnectionError;
/**
 * Thrown when operation times out.
 */
class TimeoutError extends MerkleKVError {
    constructor(message) {
        super(message);
        this.name = 'TimeoutError';
    }
}
exports.TimeoutError = TimeoutError;
/**
 * Thrown when server returns an error.
 */
class ProtocolError extends MerkleKVError {
    constructor(message) {
        super(message);
        this.name = 'ProtocolError';
    }
}
exports.ProtocolError = ProtocolError;
