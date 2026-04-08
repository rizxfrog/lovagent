"""
Interruptible inbound actor core built on top of Redis streams.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import datetime
import logging
import os
import socket
from typing import Awaitable, Callable, Dict, Optional, Sequence
from uuid import uuid4

from sqlalchemy.exc import IntegrityError

from app.graph import run_preview_graph
from app.models.actor import ActorInflightState, InboundEventDedup
from app.models.database import SessionLocal
from app.services.channel_dispatcher import channel_dispatcher
from app.services.redis_stream_bus import DlqActorEvent, InboundActorEvent, RedisStreamBus, StreamEnvelope
from app.services.runtime_config_service import runtime_config_service


logger = logging.getLogger(__name__)

GenerateReplyFn = Callable[["InboundActorTurn"], Awaitable[str]]
DeliverReplyFn = Callable[["InboundActorTurn", str], Awaitable[object]]
ConfigLoaderFn = Callable[[], Dict[str, object]]


@dataclass(frozen=True)
class InboundActorTurn:
    channel: str
    external_user_id: str
    actor_key: str
    generation_version: int
    events: tuple[InboundActorEvent, ...]


@dataclass(frozen=True)
class EnqueueResult:
    duplicate: bool
    actor_key: str
    generation_version: int


@dataclass
class _ActorRuntimeState:
    channel: str
    external_user_id: str
    actor_key: str
    generation_version: int = 0
    status: str = "collecting"
    buffer: list[InboundActorEvent] = field(default_factory=list)
    current_turn_events: list[InboundActorEvent] = field(default_factory=list)
    last_event_at: Optional[datetime] = None
    turn_task: Optional[asyncio.Task] = None
    debounce_task: Optional[asyncio.Task] = None
    generation_task: Optional[asyncio.Task] = None
    delivery_task: Optional[asyncio.Task] = None
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class InboundActorService:
    def __init__(
        self,
        *,
        bus: Optional[RedisStreamBus] = None,
        generate_reply: Optional[GenerateReplyFn] = None,
        deliver_reply: Optional[DeliverReplyFn] = None,
        config_loader: Optional[ConfigLoaderFn] = None,
        session_factory=SessionLocal,
        consumer_name: Optional[str] = None,
    ) -> None:
        self._bus = bus
        self._generate_reply = generate_reply or self._default_generate_reply
        self._deliver_reply = deliver_reply or self._default_deliver_reply
        self._config_loader = config_loader or runtime_config_service.get_effective_actor_config
        self._session_factory = session_factory
        self._consumer_name = consumer_name or f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:6]}"
        self._actors: Dict[str, _ActorRuntimeState] = {}
        self._consumer_task: Optional[asyncio.Task] = None
        self._stopping = False

    async def start(self) -> None:
        if self._consumer_task and not self._consumer_task.done():
            return

        if self._bus is None:
            self._bus = self._build_bus_from_config()
        if self._bus is None:
            logger.warning("Actor pipeline enabled but Redis is not configured; consumer loop not started")
            return

        self._stopping = False
        await self._bus.ensure_consumer_group()
        self._consumer_task = asyncio.create_task(self.consume_forever(), name="inbound-actor-consumer")

    async def stop(self) -> None:
        self._stopping = True

        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
            self._consumer_task = None

        tasks: list[asyncio.Task] = []
        for state in self._actors.values():
            for task in (state.debounce_task, state.generation_task, state.delivery_task, state.turn_task):
                if task and not task.done():
                    task.cancel()
                    tasks.append(task)
            state.debounce_task = None
            state.generation_task = None
            state.delivery_task = None
            state.turn_task = None
            state.current_turn_events = []
            state.status = "collecting"
            self._persist_actor_state(state)

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        if self._bus is not None:
            await self._bus.close()

    async def consume_forever(self) -> None:
        if self._bus is None:
            return

        while not self._stopping:
            messages = await self._bus.read_consumer_group(consumer_name=self._consumer_name)
            for message in messages:
                try:
                    await self.enqueue_event(message.event)
                except Exception:
                    logger.exception("Inbound actor event handling failed: message_id=%s", message.message_id)
                    continue
                await self._bus.ack(message.message_id)

    async def enqueue_event(self, event: InboundActorEvent) -> EnqueueResult:
        normalized_event = self._normalize_event(event)
        if not self._insert_dedup(normalized_event):
            return EnqueueResult(duplicate=True, actor_key=normalized_event.actor_key, generation_version=0)

        state = self._get_or_create_state(normalized_event.channel, normalized_event.external_user_id)
        config = self._current_config()

        async with state.lock:
            if state.status in {"generating", "delivering"}:
                self._interrupt_inflight_locked(state)

            state.buffer.append(normalized_event)
            state.last_event_at = normalized_event.occurred_at
            self._persist_actor_state(state)

            if len(state.buffer) >= int(config["actor_max_messages_per_turn"]):
                self._schedule_turn_locked(state, immediate=True)
            else:
                self._schedule_turn_locked(state, immediate=int(config["actor_debounce_ms"]) <= 0)

            return EnqueueResult(
                duplicate=False,
                actor_key=state.actor_key,
                generation_version=state.generation_version,
            )

    async def flush_actor(self, *, channel: str, external_user_id: str) -> None:
        state = self._actors.get(self.actor_key(channel, external_user_id))
        if state is None:
            return

        async with state.lock:
            self._schedule_turn_locked(state, immediate=True)

    @staticmethod
    def actor_key(channel: str, external_user_id: str) -> str:
        return f"{str(channel or '').strip().lower() or 'wecom'}:{str(external_user_id or '').strip()}"

    def _current_config(self) -> Dict[str, object]:
        config = dict(self._config_loader())
        config["actor_debounce_ms"] = max(0, int(config.get("actor_debounce_ms") or 0))
        config["actor_max_messages_per_turn"] = max(1, int(config.get("actor_max_messages_per_turn") or 1))
        config["actor_retry_max_attempts"] = max(0, int(config.get("actor_retry_max_attempts") or 0))
        config["actor_retry_backoff_base_ms"] = max(0, int(config.get("actor_retry_backoff_base_ms") or 0))
        return config

    def _build_bus_from_config(self) -> Optional[RedisStreamBus]:
        config = self._current_config()
        redis_url = str(config.get("redis_url") or "").strip()
        if not redis_url:
            return None
        return RedisStreamBus(
            redis_url=redis_url,
            redis_password=str(config.get("redis_password") or "").strip(),
        )

    def _normalize_event(self, event: InboundActorEvent) -> InboundActorEvent:
        occurred_at = event.occurred_at if isinstance(event.occurred_at, datetime) else datetime.now()
        return InboundActorEvent(
            event_id=str(event.event_id or "").strip(),
            channel=str(event.channel or "").strip().lower() or "wecom",
            external_user_id=str(event.external_user_id or "").strip(),
            payload=dict(event.payload or {}),
            attempt=max(0, int(event.attempt or 0)),
            occurred_at=occurred_at,
        )

    def _insert_dedup(self, event: InboundActorEvent) -> bool:
        db = self._session_factory()
        try:
            db.add(InboundEventDedup(event_id=event.event_id, actor_key=event.actor_key))
            db.commit()
            return True
        except IntegrityError:
            db.rollback()
            return False
        finally:
            db.close()

    def _get_or_create_state(self, channel: str, external_user_id: str) -> _ActorRuntimeState:
        key = self.actor_key(channel, external_user_id)
        state = self._actors.get(key)
        if state is not None:
            return state

        db = self._session_factory()
        try:
            persisted = (
                db.query(ActorInflightState)
                .filter(
                    ActorInflightState.channel == channel,
                    ActorInflightState.external_user_id == external_user_id,
                )
                .first()
            )
        finally:
            db.close()

        state = _ActorRuntimeState(
            channel=channel,
            external_user_id=external_user_id,
            actor_key=key,
            generation_version=int(persisted.generation_version) if persisted else 0,
            status="collecting",
            last_event_at=persisted.last_event_at if persisted else None,
        )
        self._actors[key] = state
        self._persist_actor_state(state)
        return state

    def _persist_actor_state(self, state: _ActorRuntimeState) -> None:
        db = self._session_factory()
        try:
            record = (
                db.query(ActorInflightState)
                .filter(
                    ActorInflightState.channel == state.channel,
                    ActorInflightState.external_user_id == state.external_user_id,
                )
                .first()
            )
            if record is None:
                record = ActorInflightState(channel=state.channel, external_user_id=state.external_user_id)
                db.add(record)

            record.generation_version = int(state.generation_version)
            record.status = state.status
            record.buffer_count = len(state.buffer) + len(state.current_turn_events)
            record.last_event_at = state.last_event_at
            db.commit()
        finally:
            db.close()

    def _interrupt_inflight_locked(self, state: _ActorRuntimeState) -> None:
        state.generation_version += 1
        if state.current_turn_events:
            state.buffer = [*state.current_turn_events, *state.buffer]
            state.current_turn_events = []
        if state.debounce_task and not state.debounce_task.done():
            state.debounce_task.cancel()
        if state.generation_task and not state.generation_task.done():
            state.generation_task.cancel()
        if state.delivery_task and not state.delivery_task.done():
            state.delivery_task.cancel()
        state.debounce_task = None
        state.status = "collecting"
        self._persist_actor_state(state)

    def _schedule_turn_locked(self, state: _ActorRuntimeState, *, immediate: bool) -> None:
        if self._stopping or not state.buffer:
            return
        if state.turn_task and not state.turn_task.done():
            return

        if state.debounce_task and not state.debounce_task.done():
            state.debounce_task.cancel()
            state.debounce_task = None

        if immediate:
            state.turn_task = asyncio.create_task(self._run_turn(state.actor_key))
            return

        debounce_seconds = self._current_config()["actor_debounce_ms"] / 1000.0
        state.debounce_task = asyncio.create_task(self._debounce_then_process(state.actor_key, debounce_seconds))

    async def _debounce_then_process(self, actor_key: str, delay_seconds: float) -> None:
        try:
            await asyncio.sleep(delay_seconds)
        except asyncio.CancelledError:
            return

        state = self._actors.get(actor_key)
        if state is None:
            return

        async with state.lock:
            if self._stopping:
                return
            state.debounce_task = None
            self._schedule_turn_locked(state, immediate=True)

    async def _run_turn(self, actor_key: str) -> None:
        state = self._actors.get(actor_key)
        if state is None:
            return

        async with state.lock:
            if self._stopping or not state.buffer:
                state.turn_task = None
                state.status = "collecting"
                self._persist_actor_state(state)
                return

            max_messages = int(self._current_config()["actor_max_messages_per_turn"])
            turn_events = tuple(state.buffer[:max_messages])
            state.buffer = state.buffer[max_messages:]
            state.current_turn_events = list(turn_events)
            state.status = "generating"
            turn_version = state.generation_version
            turn = InboundActorTurn(
                channel=state.channel,
                external_user_id=state.external_user_id,
                actor_key=state.actor_key,
                generation_version=turn_version,
                events=turn_events,
            )
            self._persist_actor_state(state)

        reply: Optional[str] = None
        try:
            generation_task = asyncio.create_task(self._generate_reply(turn))
            async with state.lock:
                state.generation_task = generation_task
            try:
                reply = await generation_task
            except asyncio.CancelledError:
                reply = None
            finally:
                async with state.lock:
                    if state.generation_task is generation_task:
                        state.generation_task = None

            async with state.lock:
                stale = state.generation_version != turn_version
                if stale or reply is None or self._stopping:
                    await self._finish_turn_locked(state)
                    return

                state.status = "delivering"
                self._persist_actor_state(state)

            delivery_task = asyncio.create_task(self._deliver_reply(turn, reply))
            async with state.lock:
                state.delivery_task = delivery_task
            try:
                await delivery_task
            except asyncio.CancelledError:
                pass
            finally:
                async with state.lock:
                    if state.delivery_task is delivery_task:
                        state.delivery_task = None

            async with state.lock:
                if state.generation_version == turn_version and not self._stopping:
                    state.status = "collecting"
                await self._finish_turn_locked(state)
        except Exception as exc:
            logger.exception("Inbound actor turn failed: actor=%s", actor_key)
            await self._handle_turn_failure(state, turn_events, turn_version, exc)

    async def _finish_turn_locked(self, state: _ActorRuntimeState) -> None:
        state.current_turn_events = []
        state.status = "collecting"
        state.turn_task = None
        self._persist_actor_state(state)
        if state.buffer and not self._stopping:
            self._schedule_turn_locked(state, immediate=True)

    async def _handle_turn_failure(
        self,
        state: _ActorRuntimeState,
        turn_events: Sequence[InboundActorEvent],
        turn_version: int,
        exc: Exception,
    ) -> None:
        config = self._current_config()
        retry_max_attempts = int(config["actor_retry_max_attempts"])

        retried_events: list[InboundActorEvent] = []
        dlq_events: list[DlqActorEvent] = []
        for event in turn_events:
            next_event = replace(event, attempt=event.attempt + 1)
            if next_event.attempt > retry_max_attempts:
                dlq_events.append(
                    DlqActorEvent(
                        event=next_event,
                        reason="turn_failure",
                        error_message=str(exc),
                    )
                )
            else:
                retried_events.append(next_event)

        if self._bus is not None:
            for dlq_event in dlq_events:
                try:
                    await self._bus.publish_dlq_event(dlq_event)
                except Exception:
                    logger.exception("Failed to publish DLQ event: actor=%s", state.actor_key)

        async with state.lock:
            if state.generation_version == turn_version:
                state.buffer = [*retried_events, *state.buffer]
            state.current_turn_events = []
            state.status = "collecting"
            state.turn_task = None
            self._persist_actor_state(state)
            if state.buffer and not self._stopping:
                self._schedule_turn_locked(state, immediate=True)

    async def _default_generate_reply(self, turn: InboundActorTurn) -> str:
        user_message = self._merge_user_message(turn.events)
        preview = await run_preview_graph(
            {
                "preview_mode": "reply",
                "channel": turn.channel,
                "external_user_id": turn.external_user_id,
                "user_message": user_message,
            }
        )
        return str(preview.get("reply") or "").strip()

    async def _default_deliver_reply(self, turn: InboundActorTurn, reply: str) -> object:
        return await channel_dispatcher.send_text(turn.channel, turn.external_user_id, reply)

    @staticmethod
    def _merge_user_message(events: Sequence[InboundActorEvent]) -> str:
        parts: list[str] = []
        for event in events:
            text = str(event.payload.get("text") or event.payload.get("content") or "").strip()
            if text:
                parts.append(text)
        return "\n".join(parts) if parts else "收到一条新消息"


inbound_actor_service = InboundActorService()


__all__ = [
    "EnqueueResult",
    "InboundActorEvent",
    "InboundActorService",
    "InboundActorTurn",
    "StreamEnvelope",
    "inbound_actor_service",
]
