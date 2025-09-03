"use strict";
/**
 * MerkleKV client implementation for Node.js.
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.MerkleKVClient = void 0;
const net = __importStar(require("net"));
const stream_1 = require("stream");
const errors_1 = require("./errors");
/**
 * Transform stream that splits incoming data by CRLF (\r\n).
 */
class LineTransform extends stream_1.Transform {
    constructor() {
        super(...arguments);
        this.buffer = '';
    }
    _transform(chunk, encoding, callback) {
        this.buffer += chunk.toString();
        let lines = this.buffer.split('\r\n');
        this.buffer = lines.pop() || '';
        for (const line of lines) {
            this.push(line);
        }
        callback();
    }
    _flush(callback) {
        if (this.buffer.length > 0) {
            this.push(this.buffer);
        }
        callback();
    }
}
/**
 * Client for connecting to MerkleKV server.
 *
 * This client provides a Promise-based interface for connecting to a MerkleKV server
 * and performing basic operations like GET, SET, and DELETE.
 *
 * @example
 * ```typescript
 * const client = new MerkleKVClient('localhost', 7878);
 * await client.connect();
 *
 * await client.set('user:123', 'john_doe');
 * const value = await client.get('user:123'); // Returns 'john_doe'
 * await client.delete('user:123');
 *
 * await client.close();
 * ```
 */
class MerkleKVClient {
    /**
     * Create a new MerkleKV client.
     *
     * @param host - Server hostname (default: 'localhost')
     * @param port - Server port (default: 7878)
     * @param timeout - Connection timeout in milliseconds (default: 5000)
     */
    constructor(host = 'localhost', port = 7878, timeout = 5000) {
        this.socket = null;
        this.connected = false;
        this.responseQueue = [];
        this.lineTransform = null;
        this.host = host;
        this.port = port;
        this.timeout = timeout;
    }
    /**
     * Connect to the MerkleKV server.
     *
     * @throws {ConnectionError} If connection fails
     * @throws {TimeoutError} If connection times out
     */
    async connect() {
        return new Promise((resolve, reject) => {
            const timeoutHandle = setTimeout(() => {
                if (this.socket) {
                    this.socket.destroy();
                }
                reject(new errors_1.TimeoutError(`Connection timed out after ${this.timeout}ms`));
            }, this.timeout);
            this.socket = new net.Socket();
            this.lineTransform = new LineTransform();
            // Set up the pipeline: socket -> lineTransform -> response handler
            this.socket.pipe(this.lineTransform);
            this.lineTransform.on('data', (line) => {
                this.handleResponse(line.toString());
            });
            this.socket.on('connect', () => {
                clearTimeout(timeoutHandle);
                this.connected = true;
                resolve();
            });
            this.socket.on('error', (error) => {
                clearTimeout(timeoutHandle);
                this.connected = false;
                reject(new errors_1.ConnectionError(`Failed to connect to ${this.host}:${this.port}: ${error.message}`));
            });
            this.socket.on('close', () => {
                this.connected = false;
                this.socket = null;
                // Reject any pending operations
                while (this.responseQueue.length > 0) {
                    const { reject } = this.responseQueue.shift();
                    reject(new errors_1.ConnectionError('Connection closed'));
                }
            });
            this.socket.connect(this.port, this.host);
        });
    }
    /**
     * Close the connection to the server.
     */
    async close() {
        return new Promise((resolve) => {
            if (this.socket) {
                this.socket.once('close', () => {
                    resolve();
                });
                this.socket.end();
            }
            else {
                resolve();
            }
        });
    }
    /**
     * Check if client is connected to server.
     *
     * @returns True if connected, false otherwise
     */
    isConnected() {
        return this.connected && this.socket !== null && !this.socket.destroyed;
    }
    /**
     * Handle incoming response from server.
     */
    handleResponse(response) {
        if (this.responseQueue.length === 0) {
            // Unexpected response - ignore for now
            return;
        }
        const { resolve, reject } = this.responseQueue.shift();
        // Check for protocol errors
        if (response.startsWith('ERROR ')) {
            const errorMsg = response.substring(6); // Remove 'ERROR ' prefix
            reject(new errors_1.ProtocolError(errorMsg));
            return;
        }
        resolve(response);
    }
    /**
     * Send a command to the server and return the response.
     */
    async sendCommand(command) {
        if (!this.isConnected()) {
            throw new errors_1.ConnectionError('Not connected to server. Call connect() first.');
        }
        return new Promise((resolve, reject) => {
            // Add to response queue
            this.responseQueue.push({ resolve, reject });
            // Set up timeout
            const timeoutHandle = setTimeout(() => {
                // Remove from queue if still there
                const index = this.responseQueue.findIndex(q => q.resolve === resolve);
                if (index !== -1) {
                    this.responseQueue.splice(index, 1);
                }
                reject(new errors_1.TimeoutError(`Operation timed out after ${this.timeout}ms`));
            }, this.timeout);
            // Override resolve to clear timeout
            const originalResolve = resolve;
            const wrappedResolve = (value) => {
                clearTimeout(timeoutHandle);
                originalResolve(value);
            };
            // Override reject to clear timeout
            const originalReject = reject;
            const wrappedReject = (error) => {
                clearTimeout(timeoutHandle);
                originalReject(error);
            };
            // Update queue with wrapped functions
            this.responseQueue[this.responseQueue.length - 1] = {
                resolve: wrappedResolve,
                reject: wrappedReject
            };
            // Send command with CRLF termination
            const message = `${command}\r\n`;
            this.socket.write(message, 'utf8', (error) => {
                if (error) {
                    clearTimeout(timeoutHandle);
                    // Remove from queue
                    const index = this.responseQueue.findIndex(q => q.resolve === wrappedResolve);
                    if (index !== -1) {
                        this.responseQueue.splice(index, 1);
                    }
                    reject(new errors_1.ConnectionError(`Write error: ${error.message}`));
                }
            });
        });
    }
    /**
     * Get the value for a key.
     *
     * @param key - The key to retrieve
     * @returns The value if key exists, null if key doesn't exist
     *
     * @throws {ConnectionError} If not connected or connection fails
     * @throws {TimeoutError} If operation times out
     * @throws {ProtocolError} If server returns an error
     */
    async get(key) {
        if (!key) {
            throw new Error('Key cannot be empty');
        }
        const response = await this.sendCommand(`GET ${key}`);
        if (response === 'NOT_FOUND') {
            return null;
        }
        else if (response.startsWith('VALUE ')) {
            return response.substring(6); // Remove 'VALUE ' prefix
        }
        else {
            throw new errors_1.ProtocolError(`Unexpected response: ${response}`);
        }
    }
    /**
     * Set a key-value pair.
     *
     * @param key - The key to set
     * @param value - The value to associate with the key
     * @returns True if successful
     *
     * @throws {ConnectionError} If not connected or connection fails
     * @throws {TimeoutError} If operation times out
     * @throws {ProtocolError} If server returns an error
     */
    async set(key, value) {
        if (!key) {
            throw new Error('Key cannot be empty');
        }
        // Handle empty values by quoting them
        const command = value === '' ? `SET ${key} ""` : `SET ${key} ${value}`;
        const response = await this.sendCommand(command);
        if (response === 'OK') {
            return true;
        }
        else {
            throw new errors_1.ProtocolError(`Unexpected response: ${response}`);
        }
    }
    /**
     * Delete a key.
     *
     * @param key - The key to delete
     * @returns True if successful (whether key existed or not)
     *
     * @throws {ConnectionError} If not connected or connection fails
     * @throws {TimeoutError} If operation times out
     * @throws {ProtocolError} If server returns an error
     */
    async delete(key) {
        if (!key) {
            throw new Error('Key cannot be empty');
        }
        const response = await this.sendCommand(`DELETE ${key}`);
        if (response === 'OK') {
            return true;
        }
        else {
            throw new errors_1.ProtocolError(`Unexpected response: ${response}`);
        }
    }
}
exports.MerkleKVClient = MerkleKVClient;
