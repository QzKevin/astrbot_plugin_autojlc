import unittest
from unittest.mock import AsyncMock, patch

import httpx

from jlc_client import (
    ASSETS_URL,
    SEVENTH_DAY_URL,
    SIGN_URL,
    JLCClient,
    JLCError,
    mask_account,
    validate_token,
)

TOKEN = "test-access-token-do-not-use"


def assets(beans=100):
    return {"success": True, "data": {"customerCode": "C123456", "integralVoucher": beans}}


class JLCClientTests(unittest.IsolatedAsyncioTestCase):
    def client(self, responses, *, retries=1):
        self.requests = []
        queue = list(responses)

        def handle(request):
            self.requests.append(request)
            self.assertEqual(request.headers["X-JLC-AccessToken"], TOKEN)
            value = queue.pop(0)
            if isinstance(value, Exception):
                raise value
            if isinstance(value, httpx.Response):
                return value
            return httpx.Response(200, json=value)

        return JLCClient(TOKEN, retries=retries, transport=httpx.MockTransport(handle))

    async def test_regular_sign_in(self):
        async with self.client([
            assets("100"), {"success": True, "data": {"status": True, "gainNum": "2"}}
        ]) as client:
            result = await client.sign_in()
        self.assertTrue(result.success)
        self.assertEqual(result.beans, 102)
        self.assertIn("2 个金豆", result.message)
        self.assertEqual([str(r.url) for r in self.requests], [ASSETS_URL, SIGN_URL])

    async def test_asset_balance_accepts_numeric_formats(self):
        for raw, expected in (
            (100, 100), (100.0, 100), ("100.00", 100), (" 100.0 ", 100),
            ("0.00", 0), (None, 0), ("", 0), ("  ", 0),
            (100.5, "100.5"), ("100.50", "100.5"),
            ("123456789.123456789", "123456789.123456789"),
        ):
            with self.subTest(raw=raw):
                async with self.client([assets(raw)]) as client:
                    result = await client.get_assets()
                self.assertEqual(result.beans, expected)

    async def test_invalid_asset_balance_is_not_silently_zeroed(self):
        for raw in (True, False, "bad", "NaN", "Infinity", float("inf"), [], {}):
            with self.subTest(raw=raw):
                response = assets(raw)
                # Send non-finite literals directly; httpx's JSON encoder rejects them.
                if isinstance(raw, float):
                    response = httpx.Response(
                        200,
                        text='{"success":true,"data":{"customerCode":"C123456",'
                        '"integralVoucher":Infinity}}',
                    )
                async with self.client([response]) as client:
                    with self.assertRaises(JLCError):
                        await client.get_assets()

    async def test_decimal_balance_is_added_without_rounding_or_truncation(self):
        async with self.client([
            assets("100.50"),
            {"success": True, "data": {"status": True, "gainNum": "2.00"}},
        ]) as client:
            result = await client.sign_in()
        self.assertTrue(result.success)
        self.assertEqual(result.beans, "102.5")
        self.assertIn("2 个金豆", result.message)

    async def test_whole_float_reward_is_accepted(self):
        async with self.client([
            assets(100.0), {"success": True, "data": {"status": True, "gainNum": 2.0}}
        ]) as client:
            result = await client.sign_in()
        self.assertTrue(result.success)
        self.assertEqual(result.beans, 102)

    async def test_fractional_reward_is_not_truncated(self):
        async with self.client([
            assets(), {"success": True, "data": {"status": True, "gainNum": "0.5"}}
        ]) as client:
            with self.assertRaises(JLCError):
                await client.sign_in()
        self.assertEqual(len(self.requests), 2)

    async def test_seventh_day_reward_uses_decimal_balance(self):
        async with self.client([
            assets("100.50"),
            {"success": True, "data": {"status": True, "gainNum": "0.00"}},
            {"success": True},
            assets("108.50"),
        ]) as client:
            result = await client.sign_in()
        self.assertTrue(result.success)
        self.assertEqual(result.beans, "108.5")

    async def test_already_signed_message(self):
        async with self.client([assets(), {"success": False, "message": "今日已经签到"}]) as client:
            result = await client.sign_in()
        self.assertTrue(result.success)
        self.assertEqual(result.beans, 100)

    async def test_status_false_is_already_signed(self):
        async with self.client([
            assets(), {"success": True, "data": {"status": "false"}}
        ]) as client:
            result = await client.sign_in()
        self.assertTrue(result.success)
        self.assertIn("今日已签到", result.message)
        self.assertEqual(len(self.requests), 2)

    async def test_seventh_day_string_zero_and_live_balance(self):
        async with self.client([
            assets(),
            {"success": True, "data": {"status": True, "gainNum": "0"}},
            {"success": True},
            assets(109),
        ]) as client:
            result = await client.sign_in()
        self.assertTrue(result.success)
        self.assertEqual(result.beans, 109)
        self.assertEqual(str(self.requests[2].url), SEVENTH_DAY_URL)

    async def test_seventh_day_failure_remains_pending(self):
        async with self.client([
            assets(),
            {"success": True, "data": {"status": True, "gainNum": 0}},
            {"success": False, "message": "暂时无法领取"},
        ]) as client:
            result = await client.sign_in()
        self.assertFalse(result.success)
        self.assertTrue(result.reward_pending)

    async def test_seventh_day_network_failure_remains_pending(self):
        async with self.client([
            assets(), {"success": True, "data": {"status": 1}},
            httpx.ReadTimeout("sensitive " + TOKEN),
        ]) as client:
            result = await client.sign_in()
        self.assertTrue(result.reward_pending)
        self.assertNotIn(TOKEN, result.message)

    async def test_pending_reward_can_be_claimed_after_already_signed(self):
        async with self.client([
            assets(), {"success": False, "message": "已签到"},
            {"success": True}, assets(108),
        ]) as client:
            result = await client.sign_in(pending_reward=True)
        self.assertTrue(result.success)
        self.assertEqual(result.beans, 108)
        self.assertFalse(result.reward_pending)

    async def test_pending_reward_with_false_status_can_be_claimed(self):
        async with self.client([
            assets(), {"success": True, "data": {"status": False}},
            {"success": False, "message": "已领取"}, assets(108),
        ]) as client:
            result = await client.sign_in(pending_reward=True)
        self.assertTrue(result.success)

    async def test_claim_success_is_not_lost_when_balance_query_fails(self):
        async with self.client([
            assets(), {"success": True, "data": {"status": True}},
            {"success": True}, {"success": False, "message": "查询失败"},
        ]) as client:
            result = await client.sign_in()
        self.assertTrue(result.success)
        self.assertIsNone(result.beans)

    async def test_invalid_token_does_not_attempt_sign(self):
        async with self.client([{"success": False, "message": "Token 失效"}]) as client:
            with self.assertRaisesRegex(JLCError, "Token 失效"):
                await client.sign_in()
        self.assertEqual(len(self.requests), 1)

    async def test_asset_data_does_not_override_failure_flag(self):
        value = assets()
        value["success"] = False
        async with self.client([value]) as client:
            with self.assertRaises(JLCError):
                await client.get_assets()

    async def test_missing_customer_code_rejected(self):
        async with self.client([{"data": {"integralVoucher": 100}}]) as client:
            with self.assertRaisesRegex(JLCError, "账号标识"):
                await client.get_assets()

    async def test_malformed_sign_response_is_not_a_success(self):
        for response in (
            {"success": True, "data": []},
            {"success": True, "data": {}},
            {"success": True, "data": {"status": "unknown"}},
            {"success": True, "data": {"status": True, "gainNum": "bad"}},
            {"success": True, "data": {"status": True, "gainNum": -1}},
        ):
            with self.subTest(response=response):
                async with self.client([assets(), response]) as client:
                    with self.assertRaises(JLCError):
                        await client.sign_in()

    async def test_malformed_reward_response_still_records_pending(self):
        async with self.client([
            assets(), {"success": True, "data": {"status": True}}, {"data": None}
        ]) as client:
            result = await client.sign_in()
        self.assertTrue(result.reward_pending)

    async def test_request_retries_network_and_service_errors(self):
        for failure in (httpx.ConnectError(TOKEN), httpx.Response(503), httpx.Response(429)):
            with self.subTest(failure=type(failure).__name__):
                with patch("jlc_client.asyncio.sleep", new_callable=AsyncMock) as sleep:
                    async with self.client([failure, assets()], retries=2) as client:
                        result = await client.get_assets()
                    sleep.assert_awaited_once_with(2)
                self.assertEqual(result.beans, 100)
                self.assertEqual(len(self.requests), 2)

    async def test_auth_error_is_not_retried(self):
        async with self.client([httpx.Response(401)], retries=3) as client:
            with self.assertRaisesRegex(JLCError, "凭证失效"):
                await client.get_assets()
        self.assertEqual(len(self.requests), 1)

    async def test_redirect_does_not_send_credentials_to_another_host(self):
        async with self.client([
            httpx.Response(302, headers={"Location": "https://example.com/"})
        ]) as client:
            with self.assertRaises(JLCError):
                await client.get_assets()
        self.assertEqual(len(self.requests), 1)

    async def test_unexpected_json_and_non_json_responses(self):
        for response in ([], httpx.Response(200, text="<html>login</html>")):
            async with self.client([response]) as client:
                with self.assertRaises(JLCError):
                    await client.get_assets()

    async def test_upstream_message_redacts_token(self):
        async with self.client([{"success": False, "message": "bad " + TOKEN}]) as client:
            with self.assertRaises(JLCError) as caught:
                await client.get_assets()
        self.assertNotIn(TOKEN, str(caught.exception))

    async def test_network_error_never_echoes_request(self):
        with patch("jlc_client.asyncio.sleep", new_callable=AsyncMock):
            async with self.client([httpx.ConnectError(TOKEN)] * 2, retries=2) as client:
                with self.assertRaises(JLCError) as caught:
                    await client.get_assets()
        self.assertNotIn(TOKEN, str(caught.exception))
        self.assertEqual(len(self.requests), 2)

    def test_token_validation_and_account_masking(self):
        self.assertEqual(validate_token("  abc.def-123=  "), "abc.def-123=")
        for value in ("", "a b", "a\nb", "a,b", "a;b", '"abc"', "无效", "\x7f"):
            with self.subTest(value=repr(value)):
                with self.assertRaises(JLCError):
                    validate_token(value)
        self.assertEqual(mask_account("C123456"), "C1****56")
        self.assertEqual(mask_account("1234"), "****")
