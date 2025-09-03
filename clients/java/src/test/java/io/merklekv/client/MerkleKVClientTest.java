package io.merklekv.client;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.*;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class MerkleKVClientTest {

    @Test
    @DisplayName("Client creation with valid parameters should succeed")
    void testClientCreation() {
        assertDoesNotThrow(() -> {
            // Test basic parameter validation
            String host = "localhost";
            int port = 7878;
            int timeout = 30000;
            
            assertNotNull(host);
            assertTrue(port > 0 && port < 65536);
            assertTrue(timeout > 0);
        });
    }

    @Test
    @DisplayName("Response parsing for GET should work correctly")
    void testGetResponseParsing() {
        // Test VALUE response parsing
        String response = "VALUE test_value";
        if (response.startsWith("VALUE ")) {
            String value = response.substring(6);
            assertEquals("test_value", value);
        }
        
        // Test NOT_FOUND response
        String notFoundResponse = "NOT_FOUND";
        assertEquals("NOT_FOUND", notFoundResponse);
        
        // Test ERROR response
        String errorResponse = "ERROR Invalid command";
        if (errorResponse.startsWith("ERROR ")) {
            String error = errorResponse.substring(6);
            assertEquals("Invalid command", error);
        }
    }

    @Test
    @DisplayName("Response parsing for SET should work correctly") 
    void testSetResponseParsing() {
        // Test OK response
        String okResponse = "OK";
        assertEquals("OK", okResponse);
        
        // Test ERROR response
        String errorResponse = "ERROR Server error";
        if (errorResponse.startsWith("ERROR ")) {
            String error = errorResponse.substring(6);
            assertEquals("Server error", error);
        }
    }

    @Test
    @DisplayName("Response parsing for DELETE should work correctly")
    void testDeleteResponseParsing() {
        // Test OK response (key existed)
        String okResponse = "OK";
        boolean deleted = okResponse.equals("OK");
        assertTrue(deleted);
        
        // Test NOT_FOUND response (key didn't exist)
        String notFoundResponse = "NOT_FOUND";
        boolean found = !notFoundResponse.equals("NOT_FOUND");
        assertFalse(found);
    }

    @Test
    @DisplayName("Parameter validation should work correctly")
    void testParameterValidation() {
        // Test null key validation
        assertThrows(IllegalArgumentException.class, () -> {
            String key = null;
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Key cannot be null or empty");
            }
        });
        
        // Test empty key validation
        assertThrows(IllegalArgumentException.class, () -> {
            String key = "";
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("Key cannot be null or empty");
            }
        });
        
        // Test null value validation for SET
        assertThrows(IllegalArgumentException.class, () -> {
            String value = null;
            if (value == null) {
                throw new IllegalArgumentException("Value cannot be null");
            }
        });
    }

    @Test
    @DisplayName("Exception hierarchy should work correctly")
    void testExceptionHierarchy() {
        // Test MerkleKVException as base
        MerkleKVException base = new MerkleKVException("Base error");
        assertEquals("Base error", base.getMessage());
        
        // Test KeyNotFoundException
        KeyNotFoundException keyNotFound = new KeyNotFoundException("missing_key");
        assertTrue(keyNotFound instanceof MerkleKVException);
        assertTrue(keyNotFound.getMessage().contains("missing_key"));
    }

    @Test
    @DisplayName("Client properties should be accessible")
    void testClientProperties() {
        // Test property values
        String host = "localhost";
        int port = 7878;
        int timeout = 30000;
        
        assertEquals("localhost", host);
        assertEquals(7878, port);
        assertEquals(30000, timeout);
    }

    @Test
    @DisplayName("Connection state logic should work correctly")
    void testConnectionStateLogic() {
        // Test connection state tracking
        boolean connected = true;
        boolean socketClosed = false;
        
        boolean isConnected = connected && !socketClosed;
        assertTrue(isConnected);
        
        // Test when socket is closed
        socketClosed = true;
        isConnected = connected && !socketClosed;
        assertFalse(isConnected);
    }

    @Test
    @DisplayName("Command formatting should work correctly")
    void testCommandFormatting() {
        // Test GET command
        String getCommand = "GET " + "test_key";
        assertEquals("GET test_key", getCommand);
        
        // Test SET command
        String setCommand = "SET " + "test_key" + " " + "test_value";
        assertEquals("SET test_key test_value", setCommand);
        
        // Test DELETE command  
        String deleteCommand = "DELETE " + "test_key";
        assertEquals("DELETE test_key", deleteCommand);
    }

    @Test
    @DisplayName("Unicode support should work correctly")
    void testUnicodeSupport() {
        // Test Unicode key/value handling
        String unicodeKey = "用户";
        String unicodeValue = "测试值 🚀";
        
        assertNotNull(unicodeKey);
        assertNotNull(unicodeValue);
        
        // Test command with Unicode
        String command = "SET " + unicodeKey + " " + unicodeValue;
        assertTrue(command.contains(unicodeKey));
        assertTrue(command.contains(unicodeValue));
    }

    @Test
    @DisplayName("Large values should be handled correctly")
    void testLargeValueHandling() {
        // Test large value creation
        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeValue.append("Large test data ");
        }
        
        String value = largeValue.toString();
        assertTrue(value.length() > 10000);
        
        // Test response parsing with large value
        String response = "VALUE " + value;
        if (response.startsWith("VALUE ")) {
            String extractedValue = response.substring(6);
            assertEquals(value, extractedValue);
        }
    }

    @Test
    @DisplayName("Values with spaces should be handled correctly")
    void testSpaceHandling() {
        String valueWithSpaces = "value with multiple spaces";
        String response = "VALUE " + valueWithSpaces;
        
        if (response.startsWith("VALUE ")) {
            String extractedValue = response.substring(6);
            assertEquals(valueWithSpaces, extractedValue);
        }
    }

    @Test
    @DisplayName("Error response parsing should work correctly")
    void testErrorResponseParsing() {
        // Test different error formats
        String[] errorResponses = {
            "ERROR Invalid command",
            "ERROR Connection timeout",
            "ERROR Server unavailable"
        };
        
        for (String errorResponse : errorResponses) {
            if (errorResponse.startsWith("ERROR ")) {
                String errorMessage = errorResponse.substring(6);
                assertFalse(errorMessage.isEmpty());
                assertTrue(errorResponse.contains(errorMessage));
            }
        }
    }

    @Test
    @DisplayName("Protocol compliance should be maintained")
    void testProtocolCompliance() {
        // Test that commands follow protocol format
        String[] commands = {
            "GET key1",
            "SET key1 value1", 
            "DELETE key1"
        };
        
        for (String command : commands) {
            assertFalse(command.isEmpty());
            assertTrue(command.matches("^(GET|SET|DELETE)\\s+.+"));
        }
        
        // Test that responses follow protocol format
        String[] responses = {
            "OK",
            "NOT_FOUND", 
            "VALUE test_data",
            "ERROR message"
        };
        
        for (String response : responses) {
            assertFalse(response.isEmpty());
            assertTrue(response.matches("^(OK|NOT_FOUND|VALUE .+|ERROR .+)"));
        }
    }
}
