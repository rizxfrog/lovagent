import asyncio
import unittest
from uuid import uuid4

from app.models.actor import ActorInflightState, InboundEventDedup
from app.models.database import SessionLocal, init_db
from app.services.inbound_actor_service import InboundActorEvent, InboundActorService


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

    async def _wait_for(self, predicate, timeout: float = 1.5) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if predicate():
                return
            await asyncio.sleep(0.01)
        self.fail("Timed out waiting for condition")

    def _event(self, event_id: str, text: str) -> InboundActorEvent:
        suffix = uuid4().hex[:8]
        return InboundActorEvent(
            event_id=event_id,
            channel="wecom",
            external_user_id=f"actor-user-{suffix}",
            payload={"text": text},
        )

    def _make_service(self, *, generate_reply, deliver_reply, debounce_ms: int = 0, max_messages_per_turn: int = 10):
        service = InboundActorService(
            generate_reply=generate_reply,
            deliver_reply=deliver_reply,
            config_loader=lambda: {
                "actor_pipeline_enabled": True,
                "actor_debounce_ms": debounce_ms,
                "actor_max_messages_per_turn": max_messages_per_turn,
                "actor_first_reply_delay_ms": 0,
                "actor_chunk_delay_ms": 0,
                "actor_reply_chunk_min": 1,
                "actor_reply_chunk_max": 1,
                "actor_retry_max_attempts": 0,
                "actor_retry_backoff_base_ms": 0,
                "redis_url": "",
                "redis_password": "",
            },
        )
        self._services.append(service)
        return service

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
                    external_user_id=f"overflow-user-{suffix}",
                    payload={"text": event_id},
                )
            )

        await self._wait_for(lambda: len(delivered_turns) == 2)

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


if __name__ == "__main__":
    unittest.main()
