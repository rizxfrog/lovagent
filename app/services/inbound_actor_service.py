"""
Interruptible inbound actor core built on top of Redis streams.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from datetime import datetime
import hashlib
import logging
import os
import socket
from typing import Awaitable, Callable, Dict, Literal, Optional, Sequence
from uuid import uuid4

from sqlalchemy.exc import IntegrityError

from app.graph import run_preview_graph
from app.models.actor import ActorInflightState, InboundEventDedup
from app.models.database import SessionLocal
from app.services.channel_dispatcher import channel_dispatcher
from app.services.llm_service import glm_service
from app.services.redis_stream_bus import DlqActorEvent, InboundActorEvent, RedisStreamBus, StreamEnvelope
from app.services.runtime_config_service import runtime_config_service


logger = logging.getLogger(__name__)

GenerateReplyFn = Callable[["InboundActorTurn"], Awaitable[str]]
DeliverReplyFn = Callable[["InboundActorTurn", str], Awaitable[object]]
ConfigLoaderFn = Callable[[], Dict[str, object]]
DedupState = Literal["new_pending", "existing_pending", "processed_duplicate"]


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
    ack_immediately: bool = False


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
        self._actors_guard = asyncio.Lock()
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

    async def publish_inbound_event(self, event: InboundActorEvent) -> str:
        normalized_event = self._normalize_event(event)
        if self._bus is None:
            self._bus = self._build_bus_from_config()

        if self._bus is None:
            logger.warning(
                "Actor pipeline publish fallback without Redis; processing locally: actor=%s",
                normalized_event.actor_key,
            )
            await self.enqueue_event(normalized_event)
            return ""

        return await self._bus.publish_inbound_event(normalized_event)

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

        consecutive_failures = 0
        while not self._stopping:
            try:
                config = self._current_config()
                pending_idle_ms = max(1000, int(config["actor_retry_backoff_base_ms"]))
                messages = await self._bus.reclaim_pending(
                    consumer_name=self._consumer_name,
                    min_idle_ms=pending_idle_ms,
                )
                if not messages:
                    messages = await self._bus.read_consumer_group(consumer_name=self._consumer_name)

                consecutive_failures = 0
                for message in messages:
                    try:
                        result = await self.enqueue_event(self._bind_envelope_event(message))
                    except Exception:
                        logger.exception("Inbound actor event handling failed: message_id=%s", message.message_id)
                        continue
                    if result.ack_immediately:
                        await self._bus.ack(message.message_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                consecutive_failures += 1
                delay_seconds = self._compute_consumer_backoff_seconds(consecutive_failures)
                logger.warning("Inbound actor consumer read failed, retrying in %.2fs: %s", delay_seconds, exc)
                await asyncio.sleep(delay_seconds)

    async def enqueue_event(self, event: InboundActorEvent) -> EnqueueResult:
        normalized_event = self._normalize_event(event)
        dedup_state = self._ensure_dedup_record(normalized_event)
        if dedup_state == "processed_duplicate":
            return EnqueueResult(
                duplicate=True,
                actor_key=normalized_event.actor_key,
                generation_version=0,
                ack_immediately=True,
            )
        if dedup_state == "existing_pending" and not normalized_event.source_message_id:
            return EnqueueResult(
                duplicate=True,
                actor_key=normalized_event.actor_key,
                generation_version=0,
                ack_immediately=False,
            )

        state = await self._get_or_create_state(normalized_event.channel, normalized_event.external_user_id)
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
                ack_immediately=False,
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
        config["actor_first_reply_delay_ms"] = max(0, int(config.get("actor_first_reply_delay_ms") or 0))
        config["actor_chunk_delay_ms"] = max(0, int(config.get("actor_chunk_delay_ms") or 0))
        config["actor_reply_chunk_min"] = max(1, int(config.get("actor_reply_chunk_min") or 1))
        config["actor_reply_chunk_max"] = max(
            int(config["actor_reply_chunk_min"]),
            int(config.get("actor_reply_chunk_max") or config["actor_reply_chunk_min"] or 1),
        )
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
        channel = str(event.channel or "").strip().lower() or "wecom"
        external_user_id = str(event.external_user_id or "").strip()
        payload = dict(event.payload or {})
        source_message_id = str(event.source_message_id or "").strip() or None
        event_id = str(event.event_id or "").strip()
        if not event_id:
            if source_message_id:
                event_id = f"stream:{source_message_id}"
            else:
                payload_fingerprint = hashlib.sha256(repr(sorted(payload.items())).encode("utf-8")).hexdigest()
                event_id = f"synthetic:{channel}:{external_user_id}:{occurred_at.isoformat()}:{payload_fingerprint}"
        return InboundActorEvent(
            event_id=event_id,
            channel=channel,
            external_user_id=external_user_id,
            payload=payload,
            attempt=max(0, int(event.attempt or 0)),
            occurred_at=occurred_at,
            source_message_id=source_message_id,
        )

    def _ensure_dedup_record(self, event: InboundActorEvent) -> DedupState:
        db = self._session_factory()
        try:
            record = (
                db.query(InboundEventDedup)
                .filter(
                    InboundEventDedup.event_id == event.event_id,
                    InboundEventDedup.actor_key == event.actor_key,
                )
                .first()
            )
            if record is not None:
                return "processed_duplicate" if record.processed_at is not None else "existing_pending"

            db.add(InboundEventDedup(event_id=event.event_id, actor_key=event.actor_key, processed_at=None))
            db.commit()
            return "new_pending"
        except IntegrityError:
            db.rollback()
            record = (
                db.query(InboundEventDedup)
                .filter(
                    InboundEventDedup.event_id == event.event_id,
                    InboundEventDedup.actor_key == event.actor_key,
                )
                .first()
            )
            if record is not None and record.processed_at is not None:
                return "processed_duplicate"
            return "existing_pending"
        finally:
            db.close()

    async def _get_or_create_state(self, channel: str, external_user_id: str) -> _ActorRuntimeState:
        key = self.actor_key(channel, external_user_id)
        async with self._actors_guard:
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

    def _schedule_turn_locked(self, state: _ActorRuntimeState, *, immediate: bool, delay_seconds: Optional[float] = None) -> None:
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

        debounce_seconds = delay_seconds
        if debounce_seconds is None:
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
            delivered = False
            try:
                delivery_result = await delivery_task
                delivered = self._delivery_completed(delivery_result)
            except asyncio.CancelledError:
                pass
            finally:
                async with state.lock:
                    if state.delivery_task is delivery_task:
                        state.delivery_task = None

            async with state.lock:
                if delivered and state.generation_version == turn_version and not self._stopping:
                    await self._ack_events(turn.events)
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
        exhausted_events: list[tuple[InboundActorEvent, DlqActorEvent]] = []
        for event in turn_events:
            next_event = replace(event, attempt=event.attempt + 1)
            if next_event.attempt > retry_max_attempts:
                exhausted_events.append(
                    (
                        next_event,
                        DlqActorEvent(
                            event=next_event,
                            reason="turn_failure",
                            error_message=str(exc),
                        ),
                    )
                )
            else:
                retried_events.append(next_event)

        if self._bus is not None:
            successfully_published_dlq_events: list[InboundActorEvent] = []
            for exhausted_event, dlq_event in exhausted_events:
                try:
                    await self._bus.publish_dlq_event(dlq_event)
                    successfully_published_dlq_events.append(exhausted_event)
                except Exception:
                    logger.exception("Failed to publish DLQ event: actor=%s", state.actor_key)
                    retried_events.append(exhausted_event)
            await self._ack_events(successfully_published_dlq_events)
        else:
            retried_events.extend(exhausted_event for exhausted_event, _ in exhausted_events)

        async with state.lock:
            if state.generation_version == turn_version:
                state.buffer = [*retried_events, *state.buffer]
            state.current_turn_events = []
            state.status = "collecting"
            state.turn_task = None
            self._persist_actor_state(state)
            if state.buffer and not self._stopping:
                retry_delay_seconds = self._compute_retry_backoff_seconds(retried_events)
                self._schedule_turn_locked(
                    state,
                    immediate=retry_delay_seconds <= 0,
                    delay_seconds=retry_delay_seconds if retry_delay_seconds > 0 else None,
                )

    @staticmethod
    def _bind_envelope_event(envelope: StreamEnvelope) -> InboundActorEvent:
        if envelope.event.source_message_id == envelope.message_id:
            return envelope.event
        return replace(envelope.event, source_message_id=envelope.message_id)

    async def _ack_events(self, events: Sequence[InboundActorEvent]) -> None:
        if self._bus is None:
            return self._mark_events_processed(events)

        message_ids = list(
            dict.fromkeys(
                str(event.source_message_id or "").strip()
                for event in events
                if str(event.source_message_id or "").strip()
            )
        )
        if not self._mark_events_processed(events):
            return False
        if not message_ids:
            return True
        try:
            await self._bus.ack(*message_ids)
        except Exception as exc:
            logger.warning("Inbound actor ack failed for %s: %s", message_ids, exc)
            return False
        return True

    def _mark_events_processed(self, events: Sequence[InboundActorEvent]) -> bool:
        if not events:
            return True

        db = self._session_factory()
        now = datetime.now()
        try:
            for event in events:
                record = (
                    db.query(InboundEventDedup)
                    .filter(
                        InboundEventDedup.event_id == event.event_id,
                        InboundEventDedup.actor_key == event.actor_key,
                    )
                    .first()
                )
                if record is None:
                    db.add(
                        InboundEventDedup(
                            event_id=event.event_id,
                            actor_key=event.actor_key,
                            processed_at=now,
                        )
                    )
                else:
                    record.processed_at = now
            db.commit()
            return True
        except Exception:
            db.rollback()
            logger.exception("Failed to mark inbound dedup events as processed")
            return False
        finally:
            db.close()

    def _compute_retry_backoff_seconds(self, events: Sequence[InboundActorEvent]) -> float:
        if not events:
            return 0.0
        base_ms = int(self._current_config()["actor_retry_backoff_base_ms"])
        if base_ms <= 0:
            return 0.0
        attempt = max(int(event.attempt or 0) for event in events)
        exponent = max(0, attempt - 1)
        return (base_ms * (2**exponent)) / 1000.0

    def _compute_consumer_backoff_seconds(self, consecutive_failures: int) -> float:
        base_ms = int(self._current_config()["actor_retry_backoff_base_ms"])
        if base_ms <= 0:
            base_ms = 300
        exponent = max(0, min(consecutive_failures - 1, 4))
        return (base_ms * (2**exponent)) / 1000.0

    @staticmethod
    def _delivery_completed(delivery_result: object) -> bool:
        if isinstance(delivery_result, dict):
            return str(delivery_result.get("status") or "").strip().lower() == "sent"
        status = getattr(delivery_result, "status", None)
        if isinstance(status, str):
            return status.strip().lower() == "sent"
        return True

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
        config = self._current_config()
        chunks = glm_service.plan_reply_chunks(
            reply,
            chunk_min=int(config["actor_reply_chunk_min"]),
            chunk_max=int(config["actor_reply_chunk_max"]),
        )
        return await channel_dispatcher.send_text_chunks(
            turn.channel,
            turn.external_user_id,
            chunks,
            first_delay_ms=int(config["actor_first_reply_delay_ms"]),
            chunk_delay_ms=int(config["actor_chunk_delay_ms"]),
            should_continue=self._build_delivery_guard(turn),
        )

    def _build_delivery_guard(self, turn: InboundActorTurn):
        def guard() -> bool:
            state = self._actors.get(turn.actor_key)
            if state is None:
                return False
            return not self._stopping and state.generation_version == turn.generation_version

        return guard

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
