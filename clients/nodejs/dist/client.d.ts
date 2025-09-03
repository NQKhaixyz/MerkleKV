/**
 * MerkleKV client implementation for Node.js.
 */
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
export declare class MerkleKVClient {
    private host;
    private port;
    private timeout;
    private socket;
    private connected;
    private responseQueue;
    private lineTransform;
    /**
     * Create a new MerkleKV client.
     *
     * @param host - Server hostname (default: 'localhost')
     * @param port - Server port (default: 7878)
     * @param timeout - Connection timeout in milliseconds (default: 5000)
     */
    constructor(host?: string, port?: number, timeout?: number);
    /**
     * Connect to the MerkleKV server.
     *
     * @throws {ConnectionError} If connection fails
     * @throws {TimeoutError} If connection times out
     */
    connect(): Promise<void>;
    /**
     * Close the connection to the server.
     */
    close(): Promise<void>;
    /**
     * Check if client is connected to server.
     *
     * @returns True if connected, false otherwise
     */
    isConnected(): boolean;
    /**
     * Handle incoming response from server.
     */
    private handleResponse;
    /**
     * Send a command to the server and return the response.
     */
    private sendCommand;
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
    get(key: string): Promise<string | null>;
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
    set(key: string, value: string): Promise<boolean>;
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
    delete(key: string): Promise<boolean>;
}
