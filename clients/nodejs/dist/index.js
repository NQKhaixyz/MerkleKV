"use strict";
/**
 * MerkleKV Node.js Client Library
 *
 * A TypeScript/JavaScript client for connecting to and interacting with MerkleKV servers.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.ProtocolError = exports.TimeoutError = exports.ConnectionError = exports.MerkleKVError = exports.MerkleKVClient = void 0;
var client_1 = require("./client");
Object.defineProperty(exports, "MerkleKVClient", { enumerable: true, get: function () { return client_1.MerkleKVClient; } });
var errors_1 = require("./errors");
Object.defineProperty(exports, "MerkleKVError", { enumerable: true, get: function () { return errors_1.MerkleKVError; } });
Object.defineProperty(exports, "ConnectionError", { enumerable: true, get: function () { return errors_1.ConnectionError; } });
Object.defineProperty(exports, "TimeoutError", { enumerable: true, get: function () { return errors_1.TimeoutError; } });
Object.defineProperty(exports, "ProtocolError", { enumerable: true, get: function () { return errors_1.ProtocolError; } });
