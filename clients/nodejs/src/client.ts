/**
 * MerkleKV client implementation for Node.js.
 */

import * as net from 'net';
import { Transform } from 'stream';
import { ConnectionError, TimeoutError, ProtocolError } from './errors';

/**
 * Transform stream that splits incoming data by CRLF (\r\n).
 */
class LineTransform extends Transform {
    private buffer = '';

    _transform(chunk: any, encoding: BufferEncoding, callback: Function): void {
        this.buffer += chunk.toString();
        
        let lines = this.buffer.split('\r\n');
        this.buffer = lines.pop() || '';
        
        for (const line of lines) {
            this.push(line);
        }
        
        callback();
    }

    _flush(callback: Function): void {
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
export class MerkleKVClient {
    private host: string;
    private port: number;
    private timeout: number;
    private socket: net.Socket | null = null;
    private connected = false;
    private responseQueue: Array<{ resolve: Function; reject: Function }> = [];
    private lineTransform: LineTransform | null = null;

    /**
     * Create a new MerkleKV client.
     * 
     * @param host - Server hostname (default: 'localhost')
     * @param port - Server port (default: 7878)
     * @param timeout - Connection timeout in milliseconds (default: 5000)
     */
    constructor(host = 'localhost', port = 7878, timeout = 5000) {
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
    async connect(): Promise<void> {
        return new Promise((resolve, reject) => {
            const timeoutHandle = setTimeout(() => {
                if (this.socket) {
                    this.socket.destroy();
                }
                reject(new TimeoutError(`Connection timed out after ${this.timeout}ms`));
            }, this.timeout);

            this.socket = new net.Socket();
            this.lineTransform = new LineTransform();
            
            // Set up the pipeline: socket -> lineTransform -> response handler
            this.socket.pipe(this.lineTransform);
            this.lineTransform.on('data', (line: string) => {
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
                reject(new ConnectionError(`Failed to connect to ${this.host}:${this.port}: ${error.message}`));
            });

            this.socket.on('close', () => {
                this.connected = false;
                this.socket = null;
                
                // Reject any pending operations
                while (this.responseQueue.length > 0) {
                    const { reject } = this.responseQueue.shift()!;
                    reject(new ConnectionError('Connection closed'));
                }
            });

            this.socket.connect(this.port, this.host);
        });
    }

    /**
     * Close the connection to the server.
     */
    async close(): Promise<void> {
        return new Promise((resolve) => {
            if (this.socket) {
                this.socket.once('close', () => {
                    resolve();
                });
                this.socket.end();
            } else {
                resolve();
            }
        });
    }

    /**
     * Check if client is connected to server.
     * 
     * @returns True if connected, false otherwise
     */
    isConnected(): boolean {
        return this.connected && this.socket !== null && !this.socket.destroyed;
    }

    /**
     * Handle incoming response from server.
     */
    private handleResponse(response: string): void {
        if (this.responseQueue.length === 0) {
            // Unexpected response - ignore for now
            return;
        }

        const { resolve, reject } = this.responseQueue.shift()!;
        
        // Check for protocol errors
        if (response.startsWith('ERROR ')) {
            const errorMsg = response.substring(6); // Remove 'ERROR ' prefix
            reject(new ProtocolError(errorMsg));
            return;
        }

        resolve(response);
    }

    /**
     * Send a command to the server and return the response.
     */
    private async sendCommand(command: string): Promise<string> {
        if (!this.isConnected()) {
            throw new ConnectionError('Not connected to server. Call connect() first.');
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
                reject(new TimeoutError(`Operation timed out after ${this.timeout}ms`));
            }, this.timeout);

            // Override resolve to clear timeout
            const originalResolve = resolve;
            const wrappedResolve = (value: string) => {
                clearTimeout(timeoutHandle);
                originalResolve(value);
            };

            // Override reject to clear timeout
            const originalReject = reject;
            const wrappedReject = (error: Error) => {
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
            this.socket!.write(message, 'utf8', (error) => {
                if (error) {
                    clearTimeout(timeoutHandle);
                    // Remove from queue
                    const index = this.responseQueue.findIndex(q => q.resolve === wrappedResolve);
                    if (index !== -1) {
                        this.responseQueue.splice(index, 1);
                    }
                    reject(new ConnectionError(`Write error: ${error.message}`));
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
    async get(key: string): Promise<string | null> {
        if (!key) {
            throw new Error('Key cannot be empty');
        }

        const response = await this.sendCommand(`GET ${key}`);
        
        if (response === 'NOT_FOUND') {
            return null;
        } else if (response.startsWith('VALUE ')) {
            return response.substring(6); // Remove 'VALUE ' prefix
        } else {
            throw new ProtocolError(`Unexpected response: ${response}`);
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
    async set(key: string, value: string): Promise<boolean> {
        if (!key) {
            throw new Error('Key cannot be empty');
        }

        // Handle empty values by quoting them
        const command = value === '' ? `SET ${key} ""` : `SET ${key} ${value}`;
        const response = await this.sendCommand(command);
        
        if (response === 'OK') {
            return true;
        } else {
            throw new ProtocolError(`Unexpected response: ${response}`);
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
    async delete(key: string): Promise<boolean> {
        if (!key) {
            throw new Error('Key cannot be empty');
        }

        const response = await this.sendCommand(`DELETE ${key}`);
        
        if (response === 'OK') {
            return true;
        } else {
            throw new ProtocolError(`Unexpected response: ${response}`);
        }
    }
}
