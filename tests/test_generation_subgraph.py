import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.graph.subgraphs.generation import _chat_with_retry, _preview_finalize_reply


class GenerationSubgraphTests(unittest.TestCase):
    def test_chat_with_retry_returns_empty_string_after_retry_failure(self):
        async def run_test():
            with patch(
                "app.graph.subgraphs.generation.glm_service.chat_with_context",
                AsyncMock(side_effect=httpx.ConnectError("All connection attempts failed")),
            ):
                return await _chat_with_retry(
                    system_prompt="system",
                    user_message="hello",
                    context_messages=[{"role": "assistant", "content": "context"}],
                    max_tokens=128,
                    temperature=0.88,
                    top_p=0.93,
                )

        result = asyncio.run(run_test())
        self.assertEqual(result, "")

    def test_preview_finalize_uses_natural_fallback_when_reply_is_empty(self):
        state = {
            "preview_mode": "reply",
            "user_message": "hello",
            "user_emotion": {"neutral": 1.0},
            "reply": "",
            "graph_trace": [],
        }

        with patch(
            "app.graph.subgraphs.generation.choose_natural_fallback_reply",
            return_value="fallback reply",
        ):
            result = asyncio.run(_preview_finalize_reply(state))

        self.assertEqual(result["reply"], "fallback reply")


if __name__ == "__main__":
    unittest.main()
