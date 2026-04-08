import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app.services.channel_dispatcher import ChannelDispatcher


class ChannelDispatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_send_text_chunks_applies_delays_and_sends_all_chunks(self):
        dispatcher = ChannelDispatcher()
        dispatcher.send_text = AsyncMock(return_value={"status": "sent"})

        with patch("app.services.channel_dispatcher.asyncio.sleep", AsyncMock()) as sleep_mock:
            result = await dispatcher.send_text_chunks(
                "wecom",
                "user-1",
                ["first", "second", "third"],
                first_delay_ms=120,
                chunk_delay_ms=80,
            )

        self.assertEqual(result.status, "sent")
        self.assertEqual(result.sent_chunks, 3)
        self.assertEqual(dispatcher.send_text.await_count, 3)
        self.assertEqual(sleep_mock.await_args_list[0].args[0], 0.12)
        self.assertEqual(sleep_mock.await_args_list[1].args[0], 0.08)
        self.assertEqual(sleep_mock.await_args_list[2].args[0], 0.08)

    async def test_send_text_chunks_stops_when_guard_cancels(self):
        dispatcher = ChannelDispatcher()
        dispatcher.send_text = AsyncMock(return_value={"status": "sent"})
        guard_values = iter([True, False])

        result = await dispatcher.send_text_chunks(
            "wecom",
            "user-1",
            ["first", "second"],
            should_continue=lambda: next(guard_values),
        )

        self.assertEqual(result.status, "cancelled")
        self.assertEqual(result.sent_chunks, 1)
        self.assertEqual(dispatcher.send_text.await_count, 1)

    async def test_send_text_chunks_returns_not_sent_for_empty_chunks(self):
        dispatcher = ChannelDispatcher()
        dispatcher.send_text = AsyncMock(return_value={"status": "sent"})

        result = await dispatcher.send_text_chunks(
            "wecom",
            "user-1",
            [" ", "\n", ""],
        )

        self.assertEqual(result.status, "not_sent")
        self.assertEqual(result.sent_chunks, 0)
        dispatcher.send_text.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
