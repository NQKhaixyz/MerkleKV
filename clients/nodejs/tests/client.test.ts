/**
 * Unit tests for MerkleKV Node.js client.
 */

import { MerkleKVClient, ConnectionError, TimeoutError, ProtocolError } from '../src';
import * as net from 'net';

// Mock server for testing
class MockServer {
    private server: net.Server | null = null;
    private responses: string[] = [];
    private responseIndex = 0;

    constructor(responses: string[] = []) {
        this.responses = responses;
    }

    setResponses(responses: string[]) {
        this.responses = responses;
        this.responseIndex = 0;
    }

    async start(port: number): Promise<void> {
        return new Promise((resolve) => {
            this.server = net.createServer((socket) => {
                socket.on('data', (data) => {
                    const command = data.toString().trim();
                    
                    if (this.responseIndex < this.responses.length) {
                        const response = this.responses[this.responseIndex++];
                        socket.write(`${response}\r\n`);
                    } else {
                        socket.write('ERROR Unexpected command\r\n');
                    }
                });
            });

            this.server.listen(port, () => {
                resolve();
            });
        });
    }

    async stop(): Promise<void> {
        return new Promise((resolve) => {
            if (this.server) {
                this.server.close(() => {
                    resolve();
                });
            } else {
                resolve();
            }
        });
    }
}

describe('MerkleKVClient', () => {
    let mockServer: MockServer;
    const testPort = 17878;

    beforeEach(async () => {
        mockServer = new MockServer();
        await mockServer.start(testPort);
    });

    afterEach(async () => {
        await mockServer.stop();
    });

    describe('constructor', () => {
        test('should initialize with default values', () => {
            const client = new MerkleKVClient();
            expect(client.isConnected()).toBe(false);
        });

        test('should initialize with custom values', () => {
            const client = new MerkleKVClient('example.com', 9999, 10000);
            expect(client.isConnected()).toBe(false);
        });
    });

    describe('connect', () => {
        test('should connect successfully', async () => {
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            expect(client.isConnected()).toBe(true);
            
            await client.close();
        });

        test('should throw ConnectionError on failure', async () => {
            const client = new MerkleKVClient('localhost', 65432); // Wrong port but valid
            
            await expect(client.connect()).rejects.toThrow(ConnectionError);
            expect(client.isConnected()).toBe(false);
        });

        test('should throw TimeoutError on timeout', async () => {
            const client = new MerkleKVClient('192.0.2.1', 12345, 100); // RFC5737 TEST-NET address + short timeout
            
            await expect(client.connect()).rejects.toThrow(TimeoutError);
            expect(client.isConnected()).toBe(false);
        }, 10000);
    });

    describe('close', () => {
        test('should close connection', async () => {
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            expect(client.isConnected()).toBe(true);
            
            await client.close();
            expect(client.isConnected()).toBe(false);
        });

        test('should not throw if not connected', async () => {
            const client = new MerkleKVClient();
            await expect(client.close()).resolves.not.toThrow();
        });
    });

    describe('get', () => {
        test('should return value for existing key', async () => {
            mockServer.setResponses(['VALUE test_value']);
            
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            const result = await client.get('test_key');
            expect(result).toBe('test_value');
            
            await client.close();
        });

        test('should return null for non-existent key', async () => {
            mockServer.setResponses(['NOT_FOUND']);
            
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            const result = await client.get('nonexistent_key');
            expect(result).toBeNull();
            
            await client.close();
        });

        test('should throw ProtocolError on server error', async () => {
            mockServer.setResponses(['ERROR Invalid key']);
            
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            await expect(client.get('invalid_key')).rejects.toThrow(ProtocolError);
            
            await client.close();
        });

        test('should throw error for empty key', async () => {
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            await expect(client.get('')).rejects.toThrow('Key cannot be empty');
            
            await client.close();
        });

        test('should throw ConnectionError when not connected', async () => {
            const client = new MerkleKVClient();
            
            await expect(client.get('test_key')).rejects.toThrow(ConnectionError);
        });
    });

    describe('set', () => {
        test('should set key-value pair successfully', async () => {
            mockServer.setResponses(['OK']);
            
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            const result = await client.set('test_key', 'test_value');
            expect(result).toBe(true);
            
            await client.close();
        });

        test('should handle empty value', async () => {
            mockServer.setResponses(['OK']);
            
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            const result = await client.set('test_key', '');
            expect(result).toBe(true);
            
            await client.close();
        });

        test('should throw error for empty key', async () => {
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            await expect(client.set('', 'value')).rejects.toThrow('Key cannot be empty');
            
            await client.close();
        });

        test('should throw ConnectionError when not connected', async () => {
            const client = new MerkleKVClient();
            
            await expect(client.set('test_key', 'test_value')).rejects.toThrow(ConnectionError);
        });
    });

    describe('delete', () => {
        test('should delete key successfully', async () => {
            mockServer.setResponses(['OK']);
            
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            const result = await client.delete('test_key');
            expect(result).toBe(true);
            
            await client.close();
        });

        test('should throw error for empty key', async () => {
            const client = new MerkleKVClient('localhost', testPort);
            await client.connect();
            
            await expect(client.delete('')).rejects.toThrow('Key cannot be empty');
            
            await client.close();
        });

        test('should throw ConnectionError when not connected', async () => {
            const client = new MerkleKVClient();
            
            await expect(client.delete('test_key')).rejects.toThrow(ConnectionError);
        });
    });
});
