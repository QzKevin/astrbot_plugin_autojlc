"""Exercise plugin orchestration with fake AstrBot events and real APScheduler."""

import asyncio
import importlib
import json
import sys
import tempfile
import types
import unittest
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from jlc_client import ASSETS_URL, Assets, JLCClient, JLCError, SignResult

ROOT = Path(__file__).resolve().parents[1]
PACKAGE = "_autojlc_under_test"


def decorator(*_args, **_kwargs):
    return lambda func: func


def command_group(*_args, **_kwargs):
    def wrap(func):
        func.command = decorator
        return func
    return wrap


class FakeStar:
    def __init__(self, context):
        self.context = context


class FakeChain:
    def message(self, text):
        self.text = text
        return self


def load_plugin():
    # Keep APScheduler class identities stable across the temporary AstrBot imports.
    importlib.import_module("apscheduler.schedulers.asyncio")
    package = types.ModuleType(PACKAGE)
    package.__path__ = [str(ROOT)]
    api = types.ModuleType("astrbot.api")
    api.AstrBotConfig = dict
    api.logger = MagicMock()
    event = types.ModuleType("astrbot.api.event")
    event.AstrMessageEvent = object
    event.MessageChain = FakeChain
    event.filter = types.SimpleNamespace(
        command_group=command_group,
        command=decorator,
        permission_type=decorator,
        on_astrbot_loaded=decorator,
        PermissionType=types.SimpleNamespace(ADMIN="admin"),
    )
    star = types.ModuleType("astrbot.api.star")
    star.Star = FakeStar
    star.Context = object
    star.StarTools = types.SimpleNamespace(get_data_dir=MagicMock())
    modules = {
        PACKAGE: package,
        f"{PACKAGE}.jlc_client": importlib.import_module("jlc_client"),
        f"{PACKAGE}.storage": importlib.import_module("storage"),
        "astrbot": types.ModuleType("astrbot"),
        "astrbot.api": api, "astrbot.api.event": event, "astrbot.api.star": star,
    }
    with patch.dict(sys.modules, modules):
        return importlib.import_module(f"{PACKAGE}.main")


plugin_module = load_plugin()


class FakeEvent:
    def __init__(self, sender="1", platform="test", private=True):
        self.sender, self.platform, self.private = sender, platform, private
        self.unified_msg_origin = f"{platform}:FriendMessage:{sender}"
        self.stopped = False

    def is_private_chat(self):
        return self.private

    def get_sender_id(self):
        return self.sender

    def get_platform_id(self):
        return self.platform

    def stop_event(self):
        self.stopped = True

    def plain_result(self, text):
        return text


async def collect(generator):
    return "\n".join([value async for value in generator])


def fake_client(*, assets=None, result=None, error=None):
    client = MagicMock()
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    client.get_assets = AsyncMock(
        return_value=assets or Assets("C123456", 100), side_effect=error
    )
    client.sign_in = AsyncMock(
        return_value=result or SignResult(True, "签到获得 1 个金豆", 101), side_effect=error
    )
    return client


class PluginTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        plugin_module.StarTools.get_data_dir.return_value = Path(self.temp.name)
        self.context = types.SimpleNamespace(send_message=AsyncMock(return_value=True))
        self.config = {
            "auto_sign": True, "catch_up": False,
            "account_delay_min": 0, "account_delay_max": 0,
        }
        self.plugin = plugin_module.JLCSigninPlugin(self.context, self.config)
        self.event = FakeEvent()
        self.key = self.plugin._private_key(self.event)
        self.today = self.plugin._now().date().isoformat()

    async def asyncTearDown(self):
        await self.plugin.terminate()
        await asyncio.sleep(0)

    async def bind(self, code="C123456", token="test-token", event=None):
        with patch.object(self.plugin, "_client", return_value=fake_client(
            assets=Assets(code, 100)
        )):
            return await collect(self.plugin.bind(event or self.event, token))

    async def test_bind_validates_and_updates_existing_account(self):
        self.assertIn("已绑定", await self.bind())
        self.assertIn("已更新", await self.bind(token="updated-token"))
        account = self.plugin.store.users[self.key]["accounts"][0]
        self.assertEqual(account["token"], "updated-token")
        self.assertEqual(len(self.plugin.store.users[self.key]["accounts"]), 1)

    async def test_decimal_balance_binds_signs_and_survives_storage_reload(self):
        def handle(request):
            if str(request.url) == ASSETS_URL:
                return httpx.Response(200, json={
                    "success": True,
                    "data": {"customerCode": "C123456", "integralVoucher": "100.50"},
                })
            return httpx.Response(200, json={
                "success": True, "data": {"status": True, "gainNum": "1.00"},
            })

        def create_client(token):
            return JLCClient(token, transport=httpx.MockTransport(handle))

        with patch.object(self.plugin, "_client", side_effect=create_client):
            bound = await collect(self.plugin.bind(self.event, "test-token"))
            self.assertIn("已绑定", bound)
            self.assertIn("100.5", bound)
            signed = await collect(self.plugin.sign(FakeEvent()))
            self.assertIn("101.5", signed)
        reloaded = plugin_module.UserStore(self.plugin.store.path)
        self.assertEqual(reloaded.users[self.key]["accounts"][0]["beans"], "101.5")
        self.assertEqual(
            reloaded.users[self.key]["accounts"][0]["last_sign_date"], self.today
        )

    async def test_same_sender_isolated_by_platform_instance(self):
        await self.bind()
        other = FakeEvent(platform="other")
        await self.bind(code="C765432", event=other)
        self.assertEqual(len(self.plugin.store.users), 2)
        self.assertNotEqual(self.key, self.plugin._private_key(other))

    async def test_group_commands_do_not_expose_or_mutate_accounts(self):
        await self.bind()
        group = FakeEvent(private=False)
        before = deepcopy(self.plugin.store.users)
        with patch.object(self.plugin, "_client") as client:
            commands = [
                self.plugin.bind(group, "another-token"), self.plugin.sign(group),
                self.plugin.query(group), self.plugin.me(group),
                self.plugin.switch(group, 1), self.plugin.unbind(group, "全部"),
            ]
            for command in commands:
                self.assertIn("私聊", await collect(command))
            client.assert_not_called()
        self.assertEqual(before, self.plugin.store.users)

    async def test_command_replies_are_yielded_before_stopping_event(self):
        commands = [
            ("help_cmd", ()), ("bind", ("test-token",)), ("sign", ()),
            ("sign", ("invalid",)), ("quick_sign", ()), ("query", ()),
            ("me", ()), ("switch", (1,)), ("unbind", ()),
            ("unbind", ("invalid",)), ("sign_all", ()),
        ]
        for name, args in commands:
            for private in (True, False):
                event = FakeEvent(private=private)
                with self.subTest(command=name, args=args, private=private):
                    with patch.object(self.plugin, "_client", return_value=fake_client()):
                        replies = []
                        async for text in getattr(self.plugin, name)(event, *args):
                            self.assertFalse(event.stopped, "AstrBot would drop this reply")
                            replies.append(text)
                    self.assertTrue(replies)
                    self.assertTrue(event.stopped)

    async def test_failed_binding_is_not_saved(self):
        with patch.object(self.plugin, "_client", return_value=fake_client(error=JLCError("失效"))):
            self.assertIn("绑定失败", await collect(self.plugin.bind(self.event, "test-token")))
        self.assertEqual(self.plugin.store.users, {})

    async def test_switch_unbind_and_per_user_isolation(self):
        await self.bind()
        await self.bind(code="C765432", token="second-token")
        other = FakeEvent(sender="2")
        await self.bind(event=other)
        self.assertIn("已切换", await collect(self.plugin.switch(self.event, 1)))
        self.assertEqual(self.plugin.store.users[self.key]["active_index"], 0)
        self.assertIn("无效", await collect(self.plugin.switch(self.event, 99)))
        await collect(self.plugin.unbind(self.event))
        self.assertEqual(len(self.plugin.store.users[self.key]["accounts"]), 1)
        await collect(self.plugin.unbind(self.event, "全部"))
        self.assertNotIn(self.key, self.plugin.store.users)
        self.assertIn(self.plugin._private_key(other), self.plugin.store.users)

    async def test_manual_sign_default_all_and_quick_sign(self):
        await self.bind()
        await self.bind(code="C765432", token="second-token")
        client = fake_client()
        with patch.object(self.plugin, "_client", return_value=client):
            text = await collect(self.plugin.sign(self.event))
            self.assertEqual(client.sign_in.await_count, 1)
            self.assertIn("账号 2", text)
            self.assertNotIn("test-token", text)
            await collect(self.plugin.sign(self.event, "全部"))
            self.assertEqual(client.sign_in.await_count, 3)
            await collect(self.plugin.quick_sign(self.event))
            self.assertEqual(client.sign_in.await_count, 4)
        self.assertTrue(all(
            account["last_sign_date"] == self.today
            for account in self.plugin.store.users[self.key]["accounts"]
        ))

    async def test_query_is_read_only_at_server_and_labels_local_record(self):
        await self.bind()
        client = fake_client(assets=Assets("C123456", 888))
        with patch.object(self.plugin, "_client", return_value=client):
            text = await collect(self.plugin.query(self.event))
        self.assertIn("888", text)
        self.assertIn("本地记录", text)
        client.sign_in.assert_not_awaited()

    async def test_auto_sign_skips_success_but_retries_failures(self):
        await self.bind()
        await self.bind(code="C765432", token="second-token")
        with patch.object(self.plugin, "_client", side_effect=[
            fake_client(), fake_client(error=JLCError("失效"))
        ]):
            stats = await self.plugin._run_all()
        self.assertEqual((stats["success"], stats["failed"]), (1, 1))
        client = fake_client()
        with patch.object(self.plugin, "_client", return_value=client):
            stats = await self.plugin._run_all()
        self.assertEqual((stats["success"], stats["skipped"]), (1, 1))
        client.sign_in.assert_awaited_once()
        with patch.object(self.plugin, "_client") as client:
            stats = await self.plugin._run_all()
            client.assert_not_called()
        self.assertEqual(stats["skipped"], 2)

    async def test_day_seven_pending_record_is_passed_to_retry(self):
        await self.bind()
        with patch.object(self.plugin, "_client", return_value=fake_client(
            result=SignResult(False, "待补领", reward_pending=True)
        )):
            await self.plugin._run_all()
        account = self.plugin.store.users[self.key]["accounts"][0]
        self.assertEqual(account["reward_pending_date"], self.today)
        self.assertEqual(account["last_sign_date"], "")
        client = fake_client()
        with patch.object(self.plugin, "_client", return_value=client):
            await self.plugin._run_all()
        client.sign_in.assert_awaited_once_with(pending_reward=True)
        self.assertEqual(self.plugin.store.users[self.key]["accounts"][0]["reward_pending_date"], "")

    async def test_old_pending_record_does_not_claim_on_new_day(self):
        await self.bind()
        user = deepcopy(self.plugin.store.users[self.key])
        user["accounts"][0]["reward_pending_date"] = "2000-01-01"
        self.plugin._save_user(self.key, user)
        client = fake_client()
        with patch.object(self.plugin, "_client", return_value=client):
            await self.plugin._run_all()
        client.sign_in.assert_awaited_once_with(pending_reward=False)

    async def test_notifications_use_each_binding_origin(self):
        await self.bind()
        other = FakeEvent(sender="2")
        await self.bind(code="C765432", event=other)
        with patch.object(self.plugin, "_client", return_value=fake_client()):
            await self.plugin._run_all()
        calls = self.context.send_message.await_args_list
        self.assertEqual([call.args[0] for call in calls], [
            self.event.unified_msg_origin, other.unified_msg_origin
        ])
        self.assertNotIn("C7****32", calls[0].args[1].text)
        self.assertNotIn("test-token", calls[0].args[1].text)

    async def test_push_failure_does_not_undo_success(self):
        await self.bind()
        self.context.send_message.side_effect = RuntimeError("send failed")
        with patch.object(self.plugin, "_client", return_value=fake_client()):
            stats = await self.plugin._run_all()
        self.assertEqual((stats["success"], stats["pushed"]), (1, 0))
        self.assertEqual(self.plugin.store.users[self.key]["accounts"][0]["last_sign_date"], self.today)

    async def test_push_can_be_disabled(self):
        await self.bind()
        self.config["push_result"] = False
        with patch.object(self.plugin, "_client", return_value=fake_client()):
            await self.plugin._run_all()
        self.context.send_message.assert_not_awaited()

    async def test_missing_platform_is_not_counted_as_sent(self):
        await self.bind()
        self.context.send_message.return_value = False
        with patch.object(self.plugin, "_client", return_value=fake_client()):
            stats = await self.plugin._run_all()
        self.assertEqual(stats["pushed"], 0)
        self.assertEqual(stats["success"], 1)

    async def test_manual_and_scheduled_sign_are_serialized(self):
        await self.bind()
        entered = asyncio.Event()
        release = asyncio.Event()
        client = fake_client()

        async def slow_sign(**_kwargs):
            entered.set()
            await release.wait()
            return SignResult(True, "已签到", 101)

        client.sign_in.side_effect = slow_sign
        with patch.object(self.plugin, "_client", return_value=client):
            manual = asyncio.create_task(collect(self.plugin.sign(self.event)))
            await entered.wait()
            automatic = asyncio.create_task(self.plugin._run_all())
            await asyncio.sleep(0)
            self.assertEqual(client.sign_in.await_count, 1)
            release.set()
            await manual
            stats = await automatic
        self.assertEqual(stats["skipped"], 1)
        self.assertEqual(client.sign_in.await_count, 1)

    async def test_storage_failure_is_reported_without_memory_commit(self):
        before = deepcopy(self.plugin.store.users)
        with patch.object(self.plugin, "_client", return_value=fake_client()):
            with patch("storage.os.replace", side_effect=OSError("full")):
                text = await collect(self.plugin.bind(self.event, "test-token"))
        self.assertIn("保存失败", text)
        self.assertEqual(self.plugin.store.users, before)

    async def test_scheduler_is_idempotent_hot_reloads_and_disables(self):
        self.config["sign_time"] = "01:02"
        await self.plugin.initialize()
        await self.plugin.on_loaded()
        self.assertEqual(len(self.plugin.scheduler.get_jobs()), 2)
        job = self.plugin.scheduler.get_job(plugin_module.AUTO_SIGN_JOB_ID)
        self.assertEqual(str(job.trigger.timezone), "UTC+08:00")
        self.config["sign_time"] = "03:04"
        await self.plugin._refresh_schedule()
        job = self.plugin.scheduler.get_job(plugin_module.AUTO_SIGN_JOB_ID)
        self.assertIn("hour='3', minute='4'", str(job.trigger))
        self.config["auto_sign"] = False
        await self.plugin._refresh_schedule()
        self.assertIsNone(self.plugin.scheduler.get_job(plugin_module.AUTO_SIGN_JOB_ID))
        self.assertEqual(len(self.plugin.scheduler.get_jobs()), 1)

    async def test_startup_catchup_once_and_not_a_periodic_retry(self):
        self.config.update({"sign_time": "07:23", "catch_up": True})
        now = datetime(2026, 11, 16, 10, 0, tzinfo=plugin_module.BJT)
        with patch.object(self.plugin, "_now", return_value=now):
            with patch.object(self.plugin, "_auto_sign_all", new_callable=AsyncMock) as run:
                await self.plugin.initialize()
                await asyncio.sleep(0)
                await self.plugin.on_loaded()
                await self.plugin._refresh_schedule()
                self.assertEqual(run.await_count, 1)

    async def test_startup_before_sign_time_does_not_catch_up(self):
        self.config.update({"sign_time": "23:59", "catch_up": True})
        now = datetime(2026, 11, 16, 0, 0, tzinfo=plugin_module.BJT)
        with patch.object(self.plugin, "_now", return_value=now):
            with patch.object(self.plugin, "_auto_sign_all", new_callable=AsyncMock) as run:
                await self.plugin.initialize()
                await asyncio.sleep(0)
                run.assert_not_awaited()

    async def test_disabled_auto_sign_does_not_run(self):
        self.config["auto_sign"] = False
        with patch.object(self.plugin, "_run_all", new_callable=AsyncMock) as run:
            await self.plugin._auto_sign_all()
            run.assert_not_awaited()

    async def test_bad_config_uses_safe_defaults(self):
        self.config.update({"sign_time": "99:90", "api_retries": "bad", "request_timeout": -1})
        self.assertEqual(self.plugin._schedule_settings(), (True, 7, 23))
        self.assertEqual(self.plugin._int_config("api_retries", 3, 1, 5), 3)
        self.assertEqual(self.plugin._int_config("request_timeout", 15, 3, 120), 3)

    async def test_terminate_cancels_background_work(self):
        entered = asyncio.Event()

        async def slow_run():
            entered.set()
            await asyncio.Event().wait()

        with patch.object(self.plugin, "_run_all", side_effect=slow_run):
            task = asyncio.create_task(self.plugin._auto_sign_all())
            await entered.wait()
            await self.plugin.terminate()
        self.assertTrue(task.cancelled())
        self.assertEqual(self.plugin._background_tasks, set())

    def test_config_schema_matches_defaults(self):
        schema = json.loads((ROOT / "_conf_schema.json").read_text(encoding="utf-8"))
        self.assertEqual(schema["sign_time"]["default"], "07:23")
        self.assertEqual(schema["api_retries"]["default"], 3)
