"""Atomic JSON storage; never silently replace unreadable account data."""

from __future__ import annotations

import json
import os
import tempfile
from copy import deepcopy
from pathlib import Path


class StorageError(Exception):
    pass


class UserStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.users: dict = {}
        if not self.path.exists():
            return
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict) or payload.get("version") != 1:
                raise ValueError("unsupported storage version")
            self._validate(payload.get("users"))
            self.users = payload["users"]
        except (OSError, ValueError, TypeError):
            raise StorageError("无法读取账号数据，请检查 users.json 及其备份；原文件未被覆盖。") from None

    @staticmethod
    def _validate(users: object) -> None:
        if not isinstance(users, dict):
            raise ValueError("invalid users")
        for key, user in users.items():
            if not isinstance(key, str) or not isinstance(user, dict):
                raise ValueError("invalid user")
            accounts = user.get("accounts")
            if not isinstance(accounts, list) or not accounts:
                raise ValueError("invalid accounts")
            index = user.get("active_index", 0)
            if type(index) is not int or not 0 <= index < len(accounts):
                raise ValueError("invalid active index")
            if not isinstance(user.get("umo", ""), str):
                raise ValueError("invalid message origin")
            for account in accounts:
                if not isinstance(account, dict) or any(
                    not isinstance(account.get(field), str) or not account[field]
                    for field in ("token", "customer_code")
                ):
                    raise ValueError("invalid account")

    def save(self, users: dict) -> None:
        temporary_path = None
        try:
            self._validate(users)
            snapshot = deepcopy(users)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.path.parent, prefix=".users-", delete=False
            ) as stream:
                temporary_path = Path(stream.name)
                json.dump({"version": 1, "users": snapshot}, stream, ensure_ascii=False, indent=2)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, self.path)
        except (OSError, ValueError, TypeError):
            raise StorageError("账号数据保存失败，请检查插件数据目录权限和剩余空间。") from None
        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass
        self.users = snapshot
