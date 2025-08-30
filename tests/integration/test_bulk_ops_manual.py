#!/usr/bin/env python3
"""
Manual-style tests for bulk operations in MerkleKV using the bulk_ops_server fixture.
"""

import pytest
from conftest import connect_to_server


def recv_str(client):
    return client.recv(4096).decode().strip()


def test_mset(bulk_ops_server):
    client = connect_to_server()
    try:
        client.send(b"MSET key1 value1 key2 value2 key3 value3\r\n")
        resp = recv_str(client)
        assert resp == "OK"

        client.send(b"GET key1\r\n")
        assert recv_str(client) == "VALUE value1"
        client.send(b"GET key2\r\n")
        assert recv_str(client) == "VALUE value2"
        client.send(b"GET key3\r\n")
        assert recv_str(client) == "VALUE value3"
    finally:
        client.close()


def test_mget(bulk_ops_server):
    client = connect_to_server()
    try:
        client.send(b"MGET key1 key2 key3\r\n")
        resp = recv_str(client)
        # Accept either newline or space-separated values depending on server impl
        assert "value1" in resp and "value2" in resp and "value3" in resp
    finally:
        client.close()


def test_truncate(bulk_ops_server):
    client = connect_to_server()
    try:
        client.send(b"TRUNCATE\r\n")
        resp = recv_str(client)
        assert resp == "OK"

        client.send(b"GET key1\r\n")
        assert recv_str(client) == "NOT_FOUND"
    finally:
        client.close()
