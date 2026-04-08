"""
NapCat OneBot11 forward WebSocket client service.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Optional

from app.config import settings
from app.services.redis_stream_bus import InboundActorEvent
from app.services.runtime_config_service import runtime_config_service

try:
    import websockets
except Exception:  # pragma: no cover
    websockets = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

class NapCatService:
    def __init__(self) -> None:
        self._task: Optional[asyncio.Task] = None
        self._ws = None
        self._send_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()

    def _config(self) -> dict:
        runtime = runtime_config_service.get_effective_napcat_config()
        return {
            "ws_url": runtime.get("ws_url") or settings.napcat_ws_url,
            "ws_token": runtime.get("ws_token") or settings.napcat_ws_token,
        }

    async def start(self) -> None:
        if websockets is None:
            logger.warning("NapCat disabled: websockets is unavailable")
            return
        if self._task and not self._task.done():
            return
        cfg = self._config()
        if not str(cfg["ws_url"]).strip():
            logger.warning("NapCat disabled: NAPCAT_WS_URL is empty")
            return
        '''
        this is the core logic of the NapCat service client
        1. clear the stop event to ensure the task can be restarted
        2. create a new task to run the _run_loop method
            this task will run in a loop and responsible for: connecting to the NapCat websocket server, receiving messages, and handling messages
        '''
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop())
        logger.info("napcat service started")

    async def stop(self) -> None:
        self._stop_event.set()           # 1. set stop signal
        task = self._task                # 2. save the current task reference
        self._task = None                # 3. clear task reference
        if task:                         # 4. if task exists
            task.cancel()                # 5. cancel task
            try:
                await task               # 6. wait for task to complete
            except asyncio.CancelledError:
                pass                     # 7. ignore cancellation exception
        if self._ws:                     # 8. if WebSocket connection still exists
            try:
                await self._ws.close()   # 9. close WebSocket connection
            except Exception:
                pass                     # 10. ignore close exception
            self._ws = None              # 11. clear WebSocket reference


    '''main loop'''
    async def _run_loop(self) -> None:
        delay = 1.0 # delay to reconnect in seconds
        while not self._stop_event.is_set():
            cfg = self._config()    # todo?: hot reload config maybe optimize?
            ws_url = str(cfg["ws_url"]).strip()
            if not ws_url:
                return
            headers = {}
            token = str(cfg.get("ws_token") or "").strip()
            if token:
                headers["Authorization"] = f"Bearer {token}"
            try:
                async with websockets.connect(ws_url, additional_headers=headers) as ws:
                    self._ws = ws
                    delay = 1.0
                    async for message in ws:
                        await self._handle_message(message)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("NapCat connection error: %s", exc)
            finally:
                self._ws = None

            sleep_seconds = min(30.0, delay + random.uniform(0, 0.8))
            await asyncio.sleep(sleep_seconds)
            delay = min(30.0, delay * 2)

    async def _handle_message(self, message: str) -> None:
        try:
            payload = json.loads(message)
            logger.debug("NapCat receive message: %s", message)
        except json.JSONDecodeError:
            logger.error("NapCat invalid message: %s", message)
            return

        if payload.get("post_type") != "message":
            return
        if payload.get("message_type") != "private":
            return

        content = str(payload.get("raw_message") or "").strip()
        external_user_id = str(payload.get("user_id") or "").strip()
        if not content or not external_user_id:
            return

        actor_config = runtime_config_service.get_effective_actor_config()
        if actor_config["actor_pipeline_enabled"]:
            from app.services.inbound_actor_service import inbound_actor_service

            await inbound_actor_service.publish_inbound_event(self._build_actor_event(payload, external_user_id, content))
            return

        from app.graph import run_incoming_message_graph

        await run_incoming_message_graph(
            {
                "channel": "napcat",
                "external_user_id": external_user_id,
                "user_content": content,
            }
        )

    @staticmethod
    def _build_actor_event(payload: dict, external_user_id: str, content: str) -> InboundActorEvent:
        event_id = str(payload.get("message_id") or payload.get("message_seq") or "").strip()
        return InboundActorEvent(
            event_id=event_id,
            channel="napcat",
            external_user_id=external_user_id,
            payload={
                "text": content,
                "content": content,
                "message_type": str(payload.get("message_type") or "").strip() or "private",
                "raw_message": content,
            },
        )

    async def send_private_text(self, external_user_id: str, content: str) -> None:
        if not self._ws:
            raise RuntimeError("NapCat websocket is not connected")
        payload = {
            "action": "send_private_msg",
            "params": {"user_id": int(external_user_id) if external_user_id.isdigit() else external_user_id, "message": content},
            "echo": f"lovagent-{int(asyncio.get_running_loop().time() * 1000)}",
        }
        async with self._send_lock:
            await self._ws.send(json.dumps(payload, ensure_ascii=False))


napcat_service = NapCatService()

