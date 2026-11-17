"""Async adapter for the three endpoints used by JLC-AutoSign-QingLong."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

import httpx

SIGN_URL = "https://m.jlc.com/api/activity/sign/signIn?source=3"
ASSETS_URL = "https://m.jlc.com/api/appPlatform/center/assets/selectPersonalAssetsInfo"
SEVENTH_DAY_URL = "https://m.jlc.com/api/activity/sign/receiveVoucher"
USER_AGENT = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2_1 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148 "
    "Html5Plus/1.0 (Immersed/20) JlcMobileApp"
)


class JLCError(Exception):
    """An error whose message is safe to display without request credentials."""


@dataclass(frozen=True)
class Assets:
    customer_code: str
    beans: int | str


@dataclass(frozen=True)
class SignResult:
    success: bool
    message: str
    beans: int | str | None = None
    reward_pending: bool = False


def mask_account(value: str) -> str:
    return value[:2] + "****" + value[-2:] if len(value) > 4 else "****"


def validate_token(value: str) -> str:
    token = value.strip()
    if not token or len(token) > 4096:
        raise JLCError("请提供有效的 X-JLC-AccessToken 值。")
    if not token.isascii() or any(char.isspace() for char in token):
        raise JLCError("Token 不能包含空白或非 ASCII 字符，请只复制请求头的值。")
    if any(char in token for char in (",", ";", '"', "'")) or any(
        ord(char) < 33 or ord(char) == 127 for char in token
    ):
        raise JLCError("请只填写一个 Token 的原始值，不要附加引号、Cookie 或逗号。")
    if token.lower().startswith(("x-jlc-accesstoken:", "bearer ")):
        raise JLCError("请只复制 X-JLC-AccessToken 的值，不要复制请求头名称。")
    return token


def _flag(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in ("true", "1", "false", "0"):
            return normalized in ("true", "1")
    if type(value) is int and value in (0, 1):
        return bool(value)
    raise JLCError("接口返回的状态字段无效。")


def _decimal_number(value: object) -> Decimal:
    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        raise JLCError("接口返回的金豆数量无效。")
    try:
        number = Decimal(str(value).strip())
    except InvalidOperation:
        raise JLCError("接口返回的金豆数量无效。") from None
    if not number.is_finite():
        raise JLCError("接口返回的金豆数量无效。")
    return number


def _balance(value: object) -> int | str:
    if value is None or (isinstance(value, str) and not value.strip()):
        return 0
    number = _decimal_number(value)
    if number == number.to_integral_value():
        return int(number)
    # Store fractional balances as decimal strings, preserving JSON compatibility.
    return format(number, "f").rstrip("0").rstrip(".")


def _integer(value: object) -> int:
    number = _decimal_number(value)
    if number != number.to_integral_value():
        raise JLCError("接口返回的奖励数量不是整数。")
    return int(number)


class JLCClient:
    """Use one isolated HTTP session per account operation."""

    def __init__(
        self,
        token: str,
        *,
        retries: int = 3,
        timeout: float = 15,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self._token = validate_token(token)
        self._retries = max(1, min(int(retries), 5))
        self._client = httpx.AsyncClient(
            headers={"X-JLC-AccessToken": self._token, "User-Agent": USER_AGENT},
            timeout=timeout,
            follow_redirects=False,
            transport=transport,
        )

    async def __aenter__(self) -> JLCClient:
        return self

    async def __aexit__(self, *_args) -> None:
        await self._client.aclose()

    def _message(self, result: dict, fallback: str) -> str:
        message = str(result.get("message") or fallback).replace(self._token, "[已隐藏]")
        return " ".join(message.split())[:240]

    async def _request(self, url: str) -> dict:
        for attempt in range(self._retries):
            try:
                response = await self._client.get(url)
                response.raise_for_status()
            except httpx.HTTPStatusError as error:
                status = error.response.status_code
                if status in (401, 403):
                    raise JLCError("登录凭证失效或访问被拒绝，请重新获取 Token 绑定。") from None
                if status != 429 and status < 500:
                    raise JLCError(f"接口请求失败（HTTP {status}）。") from None
                if attempt == self._retries - 1:
                    raise JLCError(f"接口暂不可用（HTTP {status}），请稍后重试。") from None
            except httpx.RequestError:
                if attempt == self._retries - 1:
                    raise JLCError(f"网络请求连续失败 {self._retries} 次，请稍后重试。") from None
            else:
                try:
                    data = response.json(parse_float=Decimal)
                except ValueError:
                    raise JLCError("接口返回了非 JSON 响应，请稍后重试。") from None
                if not isinstance(data, dict):
                    raise JLCError("接口返回的数据格式异常。")
                return data
            await asyncio.sleep(2 * (attempt + 1))
        raise JLCError("接口请求失败。")

    async def get_assets(self) -> Assets:
        result = await self._request(ASSETS_URL)
        data = result.get("data")
        if ("success" in result and not _flag(result["success"])) or not isinstance(data, dict):
            raise JLCError(self._message(result, "Token 失效或资产接口异常，请重新获取 Token。"))
        customer_code = str(data.get("customerCode") or "").strip()
        if not customer_code:
            raise JLCError("资产接口未返回账号标识，无法确认 Token 是否有效。")
        beans = _balance(data.get("integralVoucher"))
        return Assets(customer_code, beans)

    async def _claim_seventh_day(self) -> SignResult:
        # A failed claim must remain retryable even after signIn reports "already signed".
        try:
            result = await self._request(SEVENTH_DAY_URL)
            success = _flag(result.get("success"))
        except JLCError as error:
            return SignResult(False, f"签到已完成，第七天奖励待补领：{error}", reward_pending=True)
        message = self._message(result, "奖励接口异常")
        if not success and not any(
            text in message for text in ("已经领取", "已领取")
        ):
            return SignResult(False, f"第七天奖励待补领：{message}", reward_pending=True)
        try:
            beans = (await self.get_assets()).beans
        except JLCError:
            beans = None
        return SignResult(True, "第七天奖励已领取", beans=beans)

    async def sign_in(self, *, pending_reward: bool = False) -> SignResult:
        assets = await self.get_assets()
        result = await self._request(SIGN_URL)
        if not _flag(result.get("success")):
            message = self._message(result, "未知错误")
            if "已经签到" in message or "已签到" in message:
                if pending_reward:
                    return await self._claim_seventh_day()
                return SignResult(True, "今日已签到", assets.beans)
            return SignResult(False, f"签到失败：{message}", assets.beans)

        data = result.get("data")
        if not isinstance(data, dict) or "status" not in data:
            raise JLCError("签到接口返回的数据不完整，无法确认签到结果。")
        if not _flag(data["status"]):
            if pending_reward:
                return await self._claim_seventh_day()
            return SignResult(True, "今日已签到", assets.beans)

        raw_gain = data.get("gainNum")
        gain = _integer(0 if raw_gain is None else raw_gain)
        if gain < 0:
            raise JLCError("签到接口返回的奖励数量无效。")
        if gain == 0:
            return await self._claim_seventh_day()
        total = _balance(_decimal_number(assets.beans) + gain)
        return SignResult(True, f"签到获得 {gain} 个金豆", total)
