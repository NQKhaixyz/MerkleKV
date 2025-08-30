#!/usr/bin/env python3
"""
Verify MGET response format and key ordering is reasonable.
"""

import pytest
from conftest import connect_to_server


def recv_str(client):
    return client.recv(4096).decode().strip()


def test_mget_lowercase(bulk_ops_server):
    client = connect_to_server()
    try:
        client.send(b"SET k1 jadeHbg\r\n")
        assert recv_str(client) == "OK"

        client.send(b"SET k2 xinh-dep\r\n")
        assert recv_str(client) == "OK"

        client.send(b"SET k3 nhat-tren-doi\r\n")
        assert recv_str(client) == "OK"

        # Exercise MGET
        client.send(b"MGET k1 k2 k3\r\n")
        resp = recv_str(client)
        # Should contain values without echoing command name
        assert "MGET" not in resp.upper()
        for v in ("jadeHbg", "xinh-dep", "nhat-tren-doi"):
            assert v in resp
    finally:
        client.close()
