import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from app.main import app, lifespan


class MainStartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_lifespan_logs_error_when_model_check_fails(self):
        scheduler_started = asyncio.Event()
        scheduler_cancelled = asyncio.Event()

        async def fake_scheduler_loop():
            scheduler_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                scheduler_cancelled.set()
                raise

        with (
            patch("app.main.init_db"),
            patch("app.main.tunnel_service.ensure_started"),
            patch(
                "app.main.tunnel_service.get_status",
                return_value={"public_url": ""},
            ),
            patch("app.main.napcat_service.start", AsyncMock()),
            patch("app.main.napcat_service.stop", AsyncMock()),
            patch("app.main.proactive_chat_service.scheduler_loop", side_effect=fake_scheduler_loop),
            patch(
                "app.main.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": False},
            ),
            patch(
                "app.main.setup_service._check_model",
                AsyncMock(return_value={"ok": False, "detail": "connect failed"}),
            ),
            patch("app.main.logger.error") as error_mock,
        ):
            async with lifespan(app):
                await asyncio.wait_for(scheduler_started.wait(), timeout=1.0)

        await asyncio.wait_for(scheduler_cancelled.wait(), timeout=1.0)
        error_mock.assert_called_once_with("Startup model availability check failed: %s", "connect failed")


if __name__ == "__main__":
    unittest.main()
