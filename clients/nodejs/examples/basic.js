/**
 * Basic usage example for MerkleKV Node.js client.
 */

const { MerkleKVClient } = require('../dist');

async function main() {
    console.log('=== MerkleKV Node.js Client Example ===');
    
    // Create client
    const client = new MerkleKVClient('localhost', 7878);
    
    try {
        // Connect to server
        await client.connect();
        console.log('Connected to MerkleKV server');
        
        // Set some key-value pairs
        await client.set('user:123', 'john_doe');
        await client.set('user:456', 'jane_smith');
        await client.set('counter', '42');
        
        // Get values
        const user123 = await client.get('user:123');
        const user456 = await client.get('user:456');
        const counter = await client.get('counter');
        
        console.log(`user:123 = ${user123}`);
        console.log(`user:456 = ${user456}`);
        console.log(`counter = ${counter}`);
        
        // Try to get non-existent key
        const nonexistent = await client.get('does_not_exist');
        console.log(`non-existent key = ${nonexistent}`);
        
        // Delete a key
        await client.delete('user:456');
        const deletedUser = await client.get('user:456');
        console.log(`user:456 after delete = ${deletedUser}`);
        
        // Test empty value
        await client.set('empty_key', '');
        const emptyValue = await client.get('empty_key');
        console.log(`empty_key = "${emptyValue}"`);
        
        // Test Unicode
        await client.set('unicode_key', 'Hello 世界 🚀');
        const unicodeValue = await client.get('unicode_key');
        console.log(`unicode_key = ${unicodeValue}`);
        
    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        await client.close();
        console.log('Connection closed');
    }
}

// Check if this file is being run directly
if (require.main === module) {
    main().catch(console.error);
}

module.exports = { main };
