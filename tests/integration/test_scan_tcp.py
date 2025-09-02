
import pytest
from conftest import MerkleKVClient

class TestScanOverTcp:
    def test_scan_basic(self, connected_client: MerkleKVClient):
        # Clear data    
        assert connected_client.send_command("TRUNCATE") == "OK"

        # Load sample data
        assert connected_client.send_command("MSET user:1 a user:2 b user:21 c") == "OK"

        # SCAN "user:" 
        keys = connected_client.scan("user:")
        assert set(keys) == {"user:1", "user:2", "user:21"}

        # SCAN "user:2"
        keys = connected_client.scan("user:2")
        assert set(keys) == {"user:2", "user:21"}

        # SCAN "nosuch" -> empty
        keys = connected_client.scan("nosuch")
        assert keys == []

    def test_scan_is_one_argument(self, connected_client: MerkleKVClient):
        # Missing argument -> error
        resp = connected_client.send_command("SCAN")
        assert "ERROR" in resp  # server: "ERROR SCAN command requires a prefix"

        # Extra argument -> error
        resp = connected_client.send_command("SCAN user: extra")
        assert "ERROR" in resp  # server: "ERROR SCAN command accepts only one argument"

    def test_scan_special_characters(self, connected_client: MerkleKVClient):
        assert connected_client.send_command("TRUNCATE") == "OK"
        # Keys with special characters are still valid (except tab/newline)
        assert connected_client.set("proj:αβγ", "1") == "OK"
        assert connected_client.set("proj:αβδ", "2") == "OK"
        assert connected_client.set("proj:xyz", "3") == "OK"

        keys = connected_client.scan("proj:")
        # Order is not guaranteed -> compare as sets
        assert set(keys) == {"proj:αβγ", "proj:αβδ", "proj:xyz"}

    @pytest.mark.parametrize(
        "prefix,expected",
        [
            ("k:", {"k:1", "k:11", "k:111"}),
            ("k:1", {"k:1", "k:11", "k:111"}),
            ("k:11", {"k:11", "k:111"}),
            ("nope:", set()),
        ],
    )
    def test_scan_parametrized(self, connected_client: MerkleKVClient, prefix, expected):
        assert connected_client.send_command("TRUNCATE") == "OK"
        # Intentionally load nested prefix keys
        assert connected_client.send_command("MSET k:1 a k:11 b k:111 c other z") == "OK"
        keys = connected_client.scan(prefix)
        assert set(keys) == expected
