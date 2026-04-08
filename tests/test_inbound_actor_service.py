import asyncio
from dataclasses import replace
from datetime import datetime
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from app.models.actor import ActorInflightState, InboundEventDedup
from app.models.database import SessionLocal, init_db
from app.services.inbound_actor_service import InboundActorEvent, InboundActorService
from app.services.redis_stream_bus import DlqActorEvent, RedisStreamBus, StreamEnvelope


class FakeRedisStreamBus:
    def __init__(self) -> None:
        self.new_messages: list[StreamEnvelope] = []
        self.pending_messages: list[StreamEnvelope] = []
        self.acked: list[str] = []
        self.dlq_events: list[DlqActorEvent] = []
        self.dlq_failures_remaining = 0
        self.reclaim_calls = 0
        self.read_calls = 0
        self.closed = False

    async def ensure_consumer_group(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True

    async def read_consumer_group(self, *, consumer_name: str, count: int = 10, block_ms: int = 1000) -> list[StreamEnvelope]:
        self.read_calls += 1
        if self.new_messages:
            return [self.new_messages.pop(0)]
        await asyncio.sleep(0.01)
        return []

    async def reclaim_pending(
        self,
        *,
        consumer_name: str,
        min_idle_ms: int = 1000,
        count: int = 10,
        start_id: str = "0-0",
    ) -> list[StreamEnvelope]:
        self.reclaim_calls += 1
        if self.pending_messages:
            return [self.pending_messages.pop(0)]
        return []

    async def ack(self, *message_ids: str) -> int:
        self.acked.extend(message_ids)
        return len(message_ids)

    async def publish_dlq_event(self, dlq_event: DlqActorEvent) -> str:
        if self.dlq_failures_remaining > 0:
            self.dlq_failures_remaining -= 1
            raise RuntimeError("dlq unavailable")
        self.dlq_events.append(dlq_event)
        return f"dlq-{len(self.dlq_events)}"


class InboundActorServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        init_db()
        self.db = SessionLocal()
        self.db.query(InboundEventDedup).delete()
        self.db.query(ActorInflightState).delete()
        self.db.commit()
        self._services: list[InboundActorService] = []

    async def asyncTearDown(self):
        for service in self._services:
            await service.stop()

        self.db.query(InboundEventDedup).delete()
        self.db.query(ActorInflightState).delete()
        self.db.commit()
        self.db.close()

    async def _wait_for(self, predicate, timeout: float = 8.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.01)
        self.fail("Timed out waiting for condition")

    async def _wait_for_actor_idle(
        self,
        service: InboundActorService,
        *,
        channel: str,
        external_user_id: str,
        min_deliveries: int,
        delivered_turns: list[list[str]],
        timeout: float = 8.0,
    ) -> None:
        actor_key = service.actor_key(channel, external_user_id)

        def is_idle() -> bool:
            state = service._actors.get(actor_key)
            if state is None:
                return False
            if len(delivered_turns) < min_deliveries:
                return False
            return (
                state.status == "collecting"
                and not state.buffer
                and not state.current_turn_events
                and (state.turn_task is None or state.turn_task.done())
                and (state.debounce_task is None or state.debounce_task.done())
                and (state.generation_task is None or state.generation_task.done())
                and (state.delivery_task is None or state.delivery_task.done())
            )

        await self._wait_for(is_idle, timeout=timeout)

    async def _wait_for_actor_quiescent(
        self,
        service: InboundActorService,
        *,
        channel: str,
        external_user_id: str,
        timeout: float = 12.0,
    ) -> None:
        actor_key = service.actor_key(channel, external_user_id)

        def is_quiescent() -> bool:
            state = service._actors.get(actor_key)
            if state is None:
                return False
            return (
                state.status == "collecting"
                and not state.buffer
                and not state.current_turn_events
                and (state.turn_task is None or state.turn_task.done())
                and (state.debounce_task is None or state.debounce_task.done())
                and (state.generation_task is None or state.generation_task.done())
                and (state.delivery_task is None or state.delivery_task.done())
            )

        await self._wait_for(is_quiescent, timeout=timeout)

    def _event(self, event_id: str, text: str) -> InboundActorEvent:
        suffix = uuid4().hex[:8]
        return InboundActorEvent(
            event_id=event_id,
            channel="wecom",
            external_user_id=f"actor-user-{suffix}",
            payload={"text": text},
        )

    def _make_service(
        self,
        *,
        generate_reply,
        deliver_reply,
        debounce_ms: int = 0,
        max_messages_per_turn: int = 10,
        first_reply_delay_ms: int = 0,
        chunk_delay_ms: int = 0,
        reply_chunk_min: int = 1,
        reply_chunk_max: int = 1,
        retry_max_attempts: int = 0,
        retry_backoff_base_ms: int = 0,
        bus=None,
    ):
        service = InboundActorService(
            bus=bus,
            generate_reply=generate_reply,
            deliver_reply=deliver_reply,
            config_loader=lambda: {
                "actor_pipeline_enabled": True,
                "actor_debounce_ms": debounce_ms,
                "actor_max_messages_per_turn": max_messages_per_turn,
                "actor_first_reply_delay_ms": first_reply_delay_ms,
                "actor_chunk_delay_ms": chunk_delay_ms,
                "actor_reply_chunk_min": reply_chunk_min,
                "actor_reply_chunk_max": reply_chunk_max,
                "actor_retry_max_attempts": retry_max_attempts,
                "actor_retry_backoff_base_ms": retry_backoff_base_ms,
                "redis_url": "",
                "redis_password": "",
            },
        )
        self._services.append(service)
        return service

    def _envelope(
        self,
        *,
        message_id: str,
        event_id: str,
        external_user_id: str,
        text: str,
    ) -> StreamEnvelope:
        return StreamEnvelope(
            message_id=message_id,
            event=InboundActorEvent(
                event_id=event_id,
                channel="wecom",
                external_user_id=external_user_id,
                payload={"text": text},
                occurred_at=datetime(2026, 4, 9, 12, 0, 0),
            ),
        )

    def _save_dedup_record(self, *, event_id: str, actor_key: str, processed_at: datetime | None) -> None:
        record = (
            self.db.query(InboundEventDedup)
            .filter(
                InboundEventDedup.event_id == event_id,
                InboundEventDedup.actor_key == actor_key,
            )
            .first()
        )
        if record is None:
            record = InboundEventDedup(event_id=event_id, actor_key=actor_key, processed_at=processed_at)
            self.db.add(record)
        else:
            record.processed_at = processed_at
        self.db.commit()

    async def test_interrupt_generation_on_new_message(self):
        first_generation_started = asyncio.Event()
        first_generation_cancelled = asyncio.Event()
        generated_turns: list[list[str]] = []
        delivered_turns: list[list[str]] = []

        async def generate_reply(turn):
            event_ids = [event.event_id for event in turn.events]
            generated_turns.append(event_ids)
            if len(generated_turns) == 1:
                first_generation_started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    first_generation_cancelled.set()
                    raise
            return f"reply:{','.join(event_ids)}"

        async def deliver_reply(turn, reply):
            delivered_turns.append([event.event_id for event in turn.events])

        service = self._make_service(generate_reply=generate_reply, deliver_reply=deliver_reply, debounce_ms=0)
        first = self._event("evt-1", "hello")
        second = InboundActorEvent(
            event_id="evt-2",
            channel=first.channel,
            external_user_id=first.external_user_id,
            payload={"text": "again"},
        )

        await service.enqueue_event(first)
        await self._wait_for(first_generation_started.is_set)
        await service.enqueue_event(second)

        await self._wait_for(first_generation_cancelled.is_set)
        await self._wait_for(lambda: len(delivered_turns) == 1)

        self.assertEqual(generated_turns[0], ["evt-1"])
        self.assertEqual(generated_turns[1], ["evt-1", "evt-2"])
        self.assertEqual(delivered_turns, [["evt-1", "evt-2"]])

    async def test_overflow_messages_move_to_next_turn(self):
        generated_turns: list[list[str]] = []
        delivered_turns: list[list[str]] = []
        suffix = uuid4().hex[:8]
        external_user_id = f"overflow-user-{suffix}"

        async def generate_reply(turn):
            generated_turns.append([event.event_id for event in turn.events])
            await asyncio.sleep(0)
            return "ok"

        async def deliver_reply(turn, reply):
            delivered_turns.append([event.event_id for event in turn.events])

        service = self._make_service(
            generate_reply=generate_reply,
            deliver_reply=deliver_reply,
            debounce_ms=0,
            max_messages_per_turn=2,
        )

        for event_id in ("evt-1", "evt-2", "evt-3"):
            await service.enqueue_event(
                InboundActorEvent(
                    event_id=event_id,
                    channel="wecom",
                    external_user_id=external_user_id,
                    payload={"text": event_id},
                )
            )

        await self._wait_for_actor_idle(
            service,
            channel="wecom",
            external_user_id=external_user_id,
            min_deliveries=2,
            delivered_turns=delivered_turns,
        )

        self.assertEqual(generated_turns, [["evt-1", "evt-2"], ["evt-3"]])
        self.assertEqual(delivered_turns, [["evt-1", "evt-2"], ["evt-3"]])

    async def test_stale_version_guard_skips_old_delivery(self):
        first_generation_started = asyncio.Event()
        delivered_replies: list[tuple[str, list[str]]] = []

        async def generate_reply(turn):
            event_ids = [event.event_id for event in turn.events]
            if len(event_ids) == 1 and event_ids[0] == "evt-1":
                first_generation_started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    return "stale-reply"
            return "fresh-reply"

        async def deliver_reply(turn, reply):
            delivered_replies.append((reply, [event.event_id for event in turn.events]))

        service = self._make_service(generate_reply=generate_reply, deliver_reply=deliver_reply, debounce_ms=0)
        first = self._event("evt-1", "hello")
        second = InboundActorEvent(
            event_id="evt-2",
            channel=first.channel,
            external_user_id=first.external_user_id,
            payload={"text": "again"},
        )

        await service.enqueue_event(first)
        await self._wait_for(first_generation_started.is_set)
        await service.enqueue_event(second)

        await self._wait_for(lambda: len(delivered_replies) == 1)
        await asyncio.sleep(0.05)

        self.assertEqual(delivered_replies, [("fresh-reply", ["evt-1", "evt-2"])])

    async def test_stale_version_guard_stops_remaining_chunks(self):
        bus = FakeRedisStreamBus()
        sent_chunks: list[str] = []
        first_chunk_sent = asyncio.Event()
        external_user_id = f"chunk-stale-user-{uuid4().hex[:8]}"

        async def generate_reply(turn):
            return "first sentence. second sentence. third sentence."

        async def deliver_reply(turn, reply):
            return await InboundActorService._default_deliver_reply(service, turn, reply)

        async def fake_send_text(channel: str, external_user_id: str, content: str):
            sent_chunks.append(content)
            if len(sent_chunks) == 1:
                first_chunk_sent.set()
            return {"channel": channel, "status": "sent"}

        service = self._make_service(
            generate_reply=generate_reply,
            deliver_reply=deliver_reply,
            chunk_delay_ms=30,
            reply_chunk_min=3,
            reply_chunk_max=3,
            bus=bus,
        )
        event = InboundActorEvent(
            event_id="evt-chunk-stale",
            channel="wecom",
            external_user_id=external_user_id,
            payload={"text": "hello"},
            source_message_id="30-0",
        )

        with patch("app.services.inbound_actor_service.channel_dispatcher.send_text", AsyncMock(side_effect=fake_send_text)):
            await service.enqueue_event(event)
            await self._wait_for(first_chunk_sent.is_set)
            state = service._actors[service.actor_key("wecom", external_user_id)]
            async with state.lock:
                state.generation_version += 1
            await self._wait_for_actor_quiescent(
                service,
                channel="wecom",
                external_user_id=external_user_id,
            )

        self.assertEqual(len(sent_chunks), 1)
        self.assertEqual(bus.acked, [])

    async def test_ack_occurs_after_successful_processing_not_at_enqueue(self):
        bus = FakeRedisStreamBus()
        external_user_id = f"ack-user-{uuid4().hex[:8]}"
        bus.new_messages.append(
            self._envelope(
                message_id="1-0",
                event_id="evt-ack",
                external_user_id=external_user_id,
                text="hello",
            )
        )
        delivery_started = asyncio.Event()
        allow_delivery = asyncio.Event()
        delivered_turns: list[list[str]] = []

        async def generate_reply(turn):
            return "ok"

        async def deliver_reply(turn, reply):
            delivery_started.set()
            await allow_delivery.wait()
            delivered_turns.append([event.event_id for event in turn.events])

        service = self._make_service(generate_reply=generate_reply, deliver_reply=deliver_reply, bus=bus)
        await service.start()

        await self._wait_for(delivery_started.is_set)
        self.assertEqual(bus.acked, [])

        allow_delivery.set()
        await self._wait_for(lambda: bus.acked == ["1-0"])
        self.assertEqual(delivered_turns, [["evt-ack"]])

    async def test_failed_event_retries_with_backoff_then_dlq_and_ack(self):
        bus = FakeRedisStreamBus()
        external_user_id = f"retry-user-{uuid4().hex[:8]}"
        event = InboundActorEvent(
            event_id="evt-retry",
            channel="wecom",
            external_user_id=external_user_id,
            payload={"text": "retry me"},
            source_message_id="2-0",
        )
        attempt_times: list[float] = []

        async def generate_reply(turn):
            attempt_times.append(asyncio.get_running_loop().time())
            raise RuntimeError("boom")

        async def deliver_reply(turn, reply):
            self.fail("delivery should not run when generation keeps failing")

        service = self._make_service(
            generate_reply=generate_reply,
            deliver_reply=deliver_reply,
            retry_max_attempts=1,
            retry_backoff_base_ms=50,
            bus=bus,
        )
        await service.enqueue_event(event)
        await self._wait_for_actor_quiescent(
            service,
            channel="wecom",
            external_user_id=external_user_id,
        )

        self.assertEqual(len(bus.dlq_events), 1)
        self.assertEqual(bus.acked, ["2-0"])
        self.assertEqual(len(attempt_times), 2)
        self.assertGreaterEqual(attempt_times[1] - attempt_times[0], 0.045)
        self.assertEqual(bus.dlq_events[0].event.event_id, "evt-retry")
        self.assertEqual(bus.dlq_events[0].reason, "turn_failure")

    async def test_pending_recovery_path_processes_previously_unacked_entries(self):
        bus = FakeRedisStreamBus()
        external_user_id = f"pending-user-{uuid4().hex[:8]}"
        actor_key = InboundActorService.actor_key("wecom", external_user_id)
        self._save_dedup_record(event_id="stream:9-0", actor_key=actor_key, processed_at=None)
        bus.pending_messages.append(
            self._envelope(
                message_id="9-0",
                event_id="",
                external_user_id=external_user_id,
                text="from pending",
            )
        )
        delivered_turns: list[list[str]] = []

        async def generate_reply(turn):
            return "pending-ok"

        async def deliver_reply(turn, reply):
            delivered_turns.append([event.event_id for event in turn.events])

        service = self._make_service(generate_reply=generate_reply, deliver_reply=deliver_reply, bus=bus)
        await service.start()

        await self._wait_for(lambda: bus.acked == ["9-0"])
        self.assertGreater(bus.reclaim_calls, 0)
        self.assertEqual(delivered_turns, [["stream:9-0"]])

    async def test_existing_pending_dedup_does_not_enqueue_duplicate_again(self):
        generated_turns: list[list[str]] = []
        delivered_turns: list[list[str]] = []
        external_user_id = f"pending-dup-user-{uuid4().hex[:8]}"
        actor_key = InboundActorService.actor_key("wecom", external_user_id)
        self._save_dedup_record(event_id="evt-pending", actor_key=actor_key, processed_at=None)

        async def generate_reply(turn):
            generated_turns.append([event.event_id for event in turn.events])
            return "ok"

        async def deliver_reply(turn, reply):
            delivered_turns.append([event.event_id for event in turn.events])

        service = self._make_service(generate_reply=generate_reply, deliver_reply=deliver_reply, debounce_ms=0)
        result = await service.enqueue_event(
            InboundActorEvent(
                event_id="evt-pending",
                channel="wecom",
                external_user_id=external_user_id,
                payload={"text": "duplicate pending"},
            )
        )
        await asyncio.sleep(0.05)

        self.assertTrue(result.duplicate)
        self.assertFalse(result.ack_immediately)
        self.assertNotIn(actor_key, service._actors)
        self.assertEqual(generated_turns, [])
        self.assertEqual(delivered_turns, [])

    async def test_duplicate_stream_message_with_processed_dedup_is_acked_immediately(self):
        bus = FakeRedisStreamBus()
        external_user_id = f"dup-user-{uuid4().hex[:8]}"
        actor_key = InboundActorService.actor_key("wecom", external_user_id)
        generated_turns: list[list[str]] = []
        delivered_turns: list[list[str]] = []

        async def generate_reply(turn):
            generated_turns.append([event.event_id for event in turn.events])
            return "ok"

        async def deliver_reply(turn, reply):
            delivered_turns.append([event.event_id for event in turn.events])

        service = self._make_service(generate_reply=generate_reply, deliver_reply=deliver_reply, bus=bus)
        self._save_dedup_record(event_id="evt-dup", actor_key=actor_key, processed_at=datetime.now())

        bus.pending_messages.append(
            self._envelope(
                message_id="10-0",
                event_id="evt-dup",
                external_user_id=external_user_id,
                text="duplicate",
            )
        )
        await service.start()

        await self._wait_for(lambda: bus.acked == ["10-0"])
        await asyncio.sleep(0.05)

        self.assertEqual(generated_turns, [])
        self.assertEqual(delivered_turns, [])
        self.assertEqual(bus.acked, ["10-0"])

    async def test_dlq_publish_failure_retries_until_publish_succeeds_then_acks(self):
        bus = FakeRedisStreamBus()
        bus.dlq_failures_remaining = 1
        external_user_id = f"dlq-fail-user-{uuid4().hex[:8]}"
        event = InboundActorEvent(
            event_id="evt-dlq-fail",
            channel="wecom",
            external_user_id=external_user_id,
            payload={"text": "retry dlq"},
            source_message_id="20-0",
        )
        attempt_times: list[float] = []

        async def generate_reply(turn):
            attempt_times.append(asyncio.get_running_loop().time())
            raise RuntimeError("boom")

        async def deliver_reply(turn, reply):
            self.fail("delivery should not run when generation keeps failing")

        service = self._make_service(
            generate_reply=generate_reply,
            deliver_reply=deliver_reply,
            retry_max_attempts=0,
            retry_backoff_base_ms=50,
            bus=bus,
        )
        await service.enqueue_event(event)
        await self._wait_for_actor_quiescent(
            service,
            channel="wecom",
            external_user_id=external_user_id,
        )

        self.assertEqual(len(bus.dlq_events), 1)
        self.assertEqual(bus.acked, ["20-0"])
        self.assertGreaterEqual(len(attempt_times), 2)

    async def test_concurrent_first_enqueue_does_not_create_duplicate_runtime_state(self):
        suffix = uuid4().hex[:8]
        external_user_id = f"concurrent-user-{suffix}"
        delivered_turns: list[list[str]] = []

        async def generate_reply(turn):
            await asyncio.sleep(0)
            return "ok"

        async def deliver_reply(turn, reply):
            delivered_turns.append([event.event_id for event in turn.events])

        service = self._make_service(generate_reply=generate_reply, deliver_reply=deliver_reply, debounce_ms=0)
        first = InboundActorEvent(
            event_id="evt-a",
            channel="wecom",
            external_user_id=external_user_id,
            payload={"text": "a"},
        )
        second = replace(first, event_id="evt-b", payload={"text": "b"})

        await asyncio.gather(service.enqueue_event(first), service.enqueue_event(second))
        await self._wait_for_actor_idle(
            service,
            channel="wecom",
            external_user_id=external_user_id,
            min_deliveries=1,
            delivered_turns=delivered_turns,
        )

        actor_key = service.actor_key("wecom", external_user_id)
        self.assertIn(actor_key, service._actors)
        self.assertEqual(sum(1 for key in service._actors if key == actor_key), 1)
        db_count = (
            self.db.query(ActorInflightState)
            .filter(
                ActorInflightState.channel == "wecom",
                ActorInflightState.external_user_id == external_user_id,
            )
            .count()
        )
        self.assertEqual(db_count, 1)

    def test_stream_envelope_parser_tolerates_malformed_attempt(self):
        event = RedisStreamBus._deserialize_event(
            "42-0",
            {
                "event_id": "",
                "channel": "wecom",
                "external_user_id": "parse-user",
                "payload": '{"text":"hello"}',
                "attempt": "x",
                "occurred_at": "2026-04-09T12:00:00",
            },
        )

        self.assertEqual(event.event_id, "stream:42-0")
        self.assertEqual(event.attempt, 0)
        self.assertEqual(event.source_message_id, "42-0")

        negative = RedisStreamBus._deserialize_event(
            "43-0",
            {
                "event_id": "evt-negative",
                "channel": "wecom",
                "external_user_id": "parse-user",
                "payload": '{"text":"hello"}',
                "attempt": "-3",
            },
        )

        self.assertEqual(negative.attempt, 0)


if __name__ == "__main__":
    unittest.main()
