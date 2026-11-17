"""JLC daily sign-in commands and scheduling for AstrBot."""

from __future__ import annotations

import asyncio
import json
import random
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools

from .jlc_client import JLCClient, JLCError, SignResult, mask_account, validate_token
from .storage import StorageError, UserStore

BJT = timezone(timedelta(hours=8))
AUTO_SIGN_JOB_ID = "autojlc_daily_sign"
REFRESH_JOB_ID = "autojlc_refresh_schedule"
PRIVATE_MESSAGE = "请在私聊中使用此指令。绑定时只发送 X-JLC-AccessToken 的值。"
UNBOUND_MESSAGE = "尚未绑定嘉立创账号，请在私聊发送「嘉立创 绑定 <Token>」。"


class JLCSigninPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.store = UserStore(Path(StarTools.get_data_dir()) / "users.json")
        self.scheduler = AsyncIOScheduler(timezone=BJT)
        self._operation_lock = asyncio.Lock()
        self._scheduler_lock = asyncio.Lock()
        self._scheduled_settings: tuple | None = None
        self._catchup_key = ""
        self._background_tasks: set[asyncio.Task] = set()
        self._stopping = False

    def _now(self) -> datetime:
        return datetime.now(BJT)

    def _int_config(self, name: str, default: int, lower: int, upper: int) -> int:
        try:
            return max(lower, min(int(self.config.get(name, default)), upper))
        except (ValueError, TypeError):
            return default

    def _client(self, token: str) -> JLCClient:
        return JLCClient(
            token,
            retries=self._int_config("api_retries", 3, 1, 5),
            timeout=self._int_config("request_timeout", 15, 3, 120),
        )

    def _private_key(self, event: AstrMessageEvent) -> str | None:
        if not event.is_private_chat():
            return None
        return json.dumps(
            [event.get_platform_id(), str(event.get_sender_id())], ensure_ascii=False
        )

    def _save_user(self, key: str, user: dict | None) -> None:
        users = deepcopy(self.store.users)
        if user is None:
            users.pop(key, None)
        else:
            users[key] = user
        self.store.save(users)

    @staticmethod
    def _label(index: int, account: dict) -> str:
        return f"账号 {index + 1}（{mask_account(account['customer_code'])}）"

    @staticmethod
    def _format_result(result: SignResult) -> str:
        text = ("成功：" if result.success else "失败：") + result.message
        if result.beans is not None:
            text += f"，当前金豆：{result.beans}"
        return text

    async def _account_delay(self) -> None:
        minimum = self._int_config("account_delay_min", 5, 0, 300)
        maximum = self._int_config("account_delay_max", 15, 0, 300)
        await asyncio.sleep(random.uniform(min(minimum, maximum), max(minimum, maximum)))

    async def _sign_account(self, account: dict) -> SignResult:
        today = self._now().date().isoformat()
        try:
            async with self._client(account["token"]) as client:
                result = await client.sign_in(
                    pending_reward=account.get("reward_pending_date") == today
                )
        except JLCError as error:
            result = SignResult(False, str(error))
        except Exception as error:
            # Exception reprs from HTTP clients may include credentials.
            logger.error(f"嘉立创签到异常（{type(error).__name__}）")
            result = SignResult(False, "请求处理异常，请稍后重试。")
        account["last_attempt_at"] = self._now().strftime("%Y-%m-%d %H:%M:%S")
        account["last_result"] = self._format_result(result)
        if result.beans is not None:
            account["beans"] = result.beans
        if result.success:
            account["last_sign_date"] = today
            account["reward_pending_date"] = ""
        elif result.reward_pending:
            account["reward_pending_date"] = today
        return result

    async def _sign_user(
        self, key: str, user: dict, indices: list[int], *, skip_signed: bool
    ) -> tuple[list[str], int, int]:
        lines = []
        successful = 0
        skipped = 0
        for index in indices:
            account = user["accounts"][index]
            today = self._now().date().isoformat()
            if skip_signed and account.get("last_sign_date") == today:
                skipped += 1
                continue
            if lines:
                await self._account_delay()
            result = await self._sign_account(account)
            # Persist each account immediately so cancellation cannot lose a whole batch.
            self._save_user(key, user)
            successful += int(result.success)
            lines.append(f"{self._label(index, account)}：{self._format_result(result)}")
        return lines, successful, skipped

    async def _push(self, umo: str, text: str) -> bool:
        if not umo:
            return False
        try:
            delivered = await self.context.send_message(umo, MessageChain().message(text))
            if delivered is False:
                logger.warning("嘉立创结果未推送：未找到绑定会话的平台实例。")
                return False
            return True
        except Exception as error:
            logger.warning(f"嘉立创结果推送失败（{type(error).__name__}），可通过「嘉立创 我的」查看。")
            return False

    async def _run_all(self, *, force: bool = False) -> dict:
        stats = {"total": 0, "success": 0, "failed": 0, "skipped": 0, "pushed": 0}
        async with self._operation_lock:
            for key in list(self.store.users):
                if self._stopping:
                    break
                user = deepcopy(self.store.users[key])
                if stats["total"] > stats["skipped"]:
                    await self._account_delay()
                lines, successful, skipped = await self._sign_user(
                    key, user, list(range(len(user["accounts"]))), skip_signed=not force
                )
                stats["total"] += len(user["accounts"])
                stats["success"] += successful
                stats["failed"] += len(lines) - successful
                stats["skipped"] += skipped
                if lines and self.config.get("push_result", True):
                    sent = await self._push(
                        user.get("umo", ""),
                        "嘉立创每日签到结果\n"
                        + "\n".join(lines)
                        + "\n时间：" + self._now().strftime("%Y-%m-%d %H:%M:%S") + "（北京时间）",
                    )
                    stats["pushed"] += int(sent)
        logger.info(
            "嘉立创自动签到完成："
            f"成功 {stats['success']}，失败 {stats['failed']}，"
            f"今日已成功跳过 {stats['skipped']}，推送 {stats['pushed']}。"
        )
        return stats

    async def _auto_sign_all(self) -> None:
        if self._stopping or not self.config.get("auto_sign", True):
            return
        task = asyncio.current_task()
        self._background_tasks.add(task)
        try:
            await self._run_all()
        except Exception as error:
            logger.error(f"嘉立创自动签到任务失败（{type(error).__name__}），请检查数据目录和配置。")
        finally:
            self._background_tasks.discard(task)

    def _schedule_settings(self) -> tuple[bool, int, int]:
        raw = str(self.config.get("sign_time", "07:23")).strip()
        try:
            hour_text, minute_text = raw.split(":")
            hour, minute = int(hour_text), int(minute_text)
            if not 0 <= hour <= 23 or not 0 <= minute <= 59:
                raise ValueError
        except (ValueError, TypeError):
            logger.warning("嘉立创 sign_time 无效，使用默认北京时间 07:23。")
            hour, minute = 7, 23
        return bool(self.config.get("auto_sign", True)), hour, minute

    async def _refresh_schedule(self) -> None:
        async with self._scheduler_lock:
            if self._stopping:
                return
            settings = self._schedule_settings()
            enabled, hour, minute = settings
            changed = settings != self._scheduled_settings
            if changed:
                if self.scheduler.get_job(AUTO_SIGN_JOB_ID):
                    self.scheduler.remove_job(AUTO_SIGN_JOB_ID)
                if enabled:
                    self.scheduler.add_job(
                        self._auto_sign_all,
                        CronTrigger(hour=hour, minute=minute, timezone=BJT),
                        id=AUTO_SIGN_JOB_ID,
                        coalesce=True,
                        max_instances=1,
                        misfire_grace_time=3600,
                    )
                self._scheduled_settings = settings
                self._catchup_key = ""
                logger.info(
                    f"嘉立创自动签到：{'启用' if enabled else '停用'}，"
                    f"每日 {hour:02d}:{minute:02d}（北京时间）。"
                )
            if not self.scheduler.get_job(REFRESH_JOB_ID):
                self.scheduler.add_job(
                    self._refresh_schedule,
                    IntervalTrigger(seconds=30, timezone=BJT),
                    id=REFRESH_JOB_ID,
                    coalesce=True,
                    max_instances=1,
                )
            if not self.scheduler.running:
                self.scheduler.start()
            now = self._now()
            catchup_key = f"{now.date().isoformat()} {hour:02d}:{minute:02d}"
            if (
                enabled
                and changed
                and self.config.get("catch_up", True)
                and now >= now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                and self._catchup_key != catchup_key
            ):
                self._catchup_key = catchup_key
                task = asyncio.create_task(self._auto_sign_all())
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)

    @filter.command_group("嘉立创", alias={"jlc"})
    def jlc(self):
        """嘉立创签到指令。"""

    @jlc.command("帮助")
    async def help_cmd(self, event: AstrMessageEvent):
        '''嘉立创签到帮助'''
        enabled, hour, minute = self._schedule_settings()
        yield event.plain_result(
            "嘉立创自动签到\n"
            "嘉立创 绑定 <Token>：验证并绑定账号（仅私聊）\n"
            "嘉立创 签到 [全部]：签到当前账号或自己的全部账号\n"
            "嘉立创 查询：查询当前账号金豆余额和本地签到记录\n"
            "嘉立创 我的：查看账号、默认账号和最近结果\n"
            "嘉立创 切换 <序号>：切换默认账号\n"
            "嘉立创 解绑 [全部]：解绑当前账号或自己的全部账号\n"
            "嘉立创 全部签到：管理员触发全员签到\n"
            "嘉立创签到：快捷签到当前账号\n"
            "指令组别名：jlc\n"
            f"自动签到：{'开启' if enabled else '关闭'}，每日 {hour:02d}:{minute:02d}（北京时间）。\n"
            "绑定 Token 须来自嘉立创移动端请求头 X-JLC-AccessToken；更新时重新绑定即可。"
        )
        event.stop_event()

    @jlc.command("绑定")
    async def bind(self, event: AstrMessageEvent, token: str = ""):
        '''绑定嘉立创 AccessToken'''
        key = self._private_key(event)
        if key is None:
            yield event.plain_result(PRIVATE_MESSAGE)
            event.stop_event()
            return
        try:
            token = validate_token(token)
            async with self._operation_lock:
                async with self._client(token) as client:
                    assets = await client.get_assets()
                user = deepcopy(self.store.users.get(
                    key, {"accounts": [], "active_index": 0, "umo": ""}
                ))
                user["umo"] = event.unified_msg_origin
                accounts = user["accounts"]
                index = next(
                    (i for i, account in enumerate(accounts)
                     if account["customer_code"] == assets.customer_code), -1
                )
                updated = index >= 0
                if not updated:
                    accounts.append({
                        "last_sign_date": "", "reward_pending_date": "", "last_result": "尚未签到"
                    })
                    index = len(accounts) - 1
                accounts[index].update({
                    "token": token,
                    "customer_code": assets.customer_code,
                    "beans": assets.beans,
                    "bound_at": self._now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                user["active_index"] = index
                self._save_user(key, user)
                message = (
                    f"{'已更新' if updated else '已绑定'}{self._label(index, accounts[index])}，"
                    f"当前金豆：{assets.beans}。\n"
                    "已设为默认账号，发送「嘉立创 签到」测试签到。"
                )
        except (JLCError, StorageError) as error:
            message = f"绑定失败：{error}"
        except Exception as error:
            logger.error(f"嘉立创绑定异常（{type(error).__name__}）")
            message = "绑定失败：请求处理异常，请稍后重试。"
        yield event.plain_result(message)
        event.stop_event()

    @jlc.command("签到")
    async def sign(self, event: AstrMessageEvent, scope: str = ""):
        '''手动签到当前或全部嘉立创账号'''
        key = self._private_key(event)
        if key is None:
            yield event.plain_result(PRIVATE_MESSAGE)
            event.stop_event()
            return
        if scope not in ("", "全部", "all"):
            yield event.plain_result("用法：嘉立创 签到 [全部]")
            event.stop_event()
            return
        try:
            async with self._operation_lock:
                user = deepcopy(self.store.users.get(key))
                if not user:
                    message = UNBOUND_MESSAGE
                else:
                    indices = (
                        list(range(len(user["accounts"]))) if scope
                        else [user["active_index"]]
                    )
                    lines, _, _ = await self._sign_user(key, user, indices, skip_signed=False)
                    message = "\n".join(lines)
        except StorageError as error:
            message = f"签到请求已处理，但结果未能保存：{error} 请发送「嘉立创 查询」核实。"
        yield event.plain_result(message)
        event.stop_event()

    @filter.command("嘉立创签到", alias={"jlc签到"})
    async def quick_sign(self, event: AstrMessageEvent):
        '''快捷签到当前嘉立创账号'''
        async for result in self.sign(event):
            yield result

    @jlc.command("查询")
    async def query(self, event: AstrMessageEvent):
        '''查询当前嘉立创账号的金豆余额和签到记录'''
        key = self._private_key(event)
        if key is None:
            yield event.plain_result(PRIVATE_MESSAGE)
            event.stop_event()
            return
        try:
            async with self._operation_lock:
                user = deepcopy(self.store.users.get(key))
                if not user:
                    message = UNBOUND_MESSAGE
                else:
                    index = user["active_index"]
                    account = user["accounts"][index]
                    async with self._client(account["token"]) as client:
                        assets = await client.get_assets()
                    account["beans"] = assets.beans
                    self._save_user(key, user)
                    message = (
                        f"{self._label(index, account)}\n当前金豆：{assets.beans}\n"
                        f"上次成功签到：{account.get('last_sign_date') or '暂无记录'}\n"
                        f"最近结果：{account.get('last_result', '尚未签到')}\n"
                        "签到日期为本插件的本地记录，不代表其他客户端的实时签到状态。"
                    )
        except (JLCError, StorageError) as error:
            message = f"查询失败：{error}"
        except Exception as error:
            logger.error(f"嘉立创查询异常（{type(error).__name__}）")
            message = "查询失败：请求处理异常，请稍后重试。"
        yield event.plain_result(message)
        event.stop_event()

    @jlc.command("我的")
    async def me(self, event: AstrMessageEvent):
        '''查看已绑定的嘉立创账号'''
        key = self._private_key(event)
        if key is None:
            yield event.plain_result(PRIVATE_MESSAGE)
            event.stop_event()
            return
        user = self.store.users.get(key)
        if not user:
            yield event.plain_result(UNBOUND_MESSAGE)
            event.stop_event()
            return
        lines = [f"已绑定 {len(user['accounts'])} 个嘉立创账号"]
        for index, account in enumerate(user["accounts"]):
            active = " [当前]" if index == user["active_index"] else ""
            lines.append(
                f"{self._label(index, account)}{active}\n"
                f"  金豆（上次记录）：{account.get('beans', '未知')}\n"
                f"  绑定时间：{account.get('bound_at', '未知')}\n"
                f"  最近尝试：{account.get('last_attempt_at', '暂无')}\n"
                f"  最近结果：{account.get('last_result', '尚未签到')}"
            )
        yield event.plain_result("\n".join(lines))
        event.stop_event()

    @jlc.command("切换")
    async def switch(self, event: AstrMessageEvent, number: int = 0):
        '''切换默认嘉立创账号'''
        key = self._private_key(event)
        if key is None:
            yield event.plain_result(PRIVATE_MESSAGE)
            event.stop_event()
            return
        try:
            async with self._operation_lock:
                user = deepcopy(self.store.users.get(key))
                if not user:
                    message = UNBOUND_MESSAGE
                elif not 1 <= number <= len(user["accounts"]):
                    message = "账号序号无效。发送「嘉立创 我的」查看序号，再发送「嘉立创 切换 <序号>」。"
                else:
                    user["active_index"] = number - 1
                    self._save_user(key, user)
                    message = f"已切换到{self._label(number - 1, user['accounts'][number - 1])}。"
        except StorageError as error:
            message = f"切换失败：{error}"
        yield event.plain_result(message)
        event.stop_event()

    @jlc.command("解绑")
    async def unbind(self, event: AstrMessageEvent, scope: str = ""):
        '''解绑当前或全部嘉立创账号'''
        key = self._private_key(event)
        if key is None:
            yield event.plain_result(PRIVATE_MESSAGE)
            event.stop_event()
            return
        if scope not in ("", "全部", "all"):
            yield event.plain_result("用法：嘉立创 解绑 [全部]")
            event.stop_event()
            return
        try:
            async with self._operation_lock:
                user = deepcopy(self.store.users.get(key))
                if not user:
                    message = UNBOUND_MESSAGE
                elif scope:
                    self._save_user(key, None)
                    message = "已解绑你的全部嘉立创账号。"
                else:
                    account = user["accounts"].pop(user["active_index"])
                    user["active_index"] = 0
                    self._save_user(key, user if user["accounts"] else None)
                    message = f"已解绑账号（{mask_account(account['customer_code'])}）。"
        except StorageError as error:
            message = f"解绑失败：{error}"
        yield event.plain_result(message)
        event.stop_event()

    @filter.permission_type(filter.PermissionType.ADMIN)
    @jlc.command("全部签到")
    async def sign_all(self, event: AstrMessageEvent):
        '''管理员：立即为所有绑定账号签到'''
        yield event.plain_result("开始为全部绑定账号签到，结果将按配置私聊推送，请稍候。")
        try:
            stats = await self._run_all(force=True)
            message = (
                f"全员签到完成：共 {stats['total']} 个账号，"
                f"成功 {stats['success']}，失败 {stats['failed']}，"
                f"已推送 {stats['pushed']} 位用户。"
            )
        except Exception as error:
            logger.error(f"嘉立创全员签到异常（{type(error).__name__}）")
            message = "全员签到中断，请检查数据目录和配置；已保存的结果可通过「嘉立创 我的」查看。"
        yield event.plain_result(message)
        event.stop_event()

    async def initialize(self):
        await self._refresh_schedule()

    @filter.on_astrbot_loaded()
    async def on_loaded(self):
        await self._refresh_schedule()

    async def terminate(self):
        self._stopping = True
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)
        tasks = [task for task in self._background_tasks if task is not asyncio.current_task()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()
