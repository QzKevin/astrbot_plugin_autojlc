import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from storage import StorageError, UserStore


def users():
    return {
        "platform-user": {
            "accounts": [{"token": "test-token", "customer_code": "C123456"}],
            "active_index": 0,
            "umo": "test:FriendMessage:1",
        }
    }


class UserStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "plugin-data" / "users.json"
        self.store = UserStore(self.path)

    def test_round_trip_and_detached_snapshot(self):
        value = users()
        self.store.save(value)
        value["platform-user"]["accounts"][0]["token"] = "changed"
        loaded = UserStore(self.path)
        self.assertEqual(loaded.users, users())
        self.assertEqual(self.store.users, users())
        self.assertEqual(list(self.path.parent.glob(".users-*")), [])

    def test_failed_replace_keeps_previous_file_and_memory(self):
        self.store.save(users())
        before = self.path.read_bytes()
        with patch("storage.os.replace", side_effect=OSError("disk full")):
            with self.assertRaises(StorageError):
                self.store.save({})
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.store.users, users())
        self.assertEqual(list(self.path.parent.glob(".users-*")), [])

    def test_corrupt_file_is_never_overwritten(self):
        self.path.parent.mkdir()
        self.path.write_text("{broken", encoding="utf-8")
        with self.assertRaises(StorageError):
            UserStore(self.path)
        self.assertEqual(self.path.read_text(encoding="utf-8"), "{broken")

    def test_invalid_schema_does_not_load(self):
        self.path.parent.mkdir()
        for payload in (
            [], {"version": 2, "users": {}}, {"version": 1, "users": []},
            {"version": 1, "users": {"a": {"accounts": []}}},
            {"version": 1, "users": {"a": {
                "accounts": [{"token": "", "customer_code": "1"}], "active_index": 0
            }}},
        ):
            with self.subTest(payload=payload):
                self.path.write_text(json.dumps(payload), encoding="utf-8")
                with self.assertRaises(StorageError):
                    UserStore(self.path)
