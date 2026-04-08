"""
Redis stream bus wrapper for the inbound actor pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import json
import logging
from typing import Any, Dict, List, Optional


logger = logging.getLogger(__name__)

DEFAULT_INBOUND_STREAM = "actor:inbound"
DEFAULT_DLQ_STREAM = "actor:inbound:dlq"
DEFAULT_CONSUMER_GROUP = "lovagent-actor"


def _load_redis_modules():
    try:
        from redis.asyncio import Redis
        from redis.exceptions import ResponseError
    except Exception as exc:  # pragma: no cover - exercised only when dependency is missing
        raise RuntimeError("Redis dependency is not available") from exc
    return Redis, ResponseError


@dataclass(frozen=True)
class InboundActorEvent:
    event_id: str
    channel: str
    external_user_id: str
    payload: Dict[str, object] = field(default_factory=dict)
    attempt: int = 0
    occurred_at: datetime = field(default_factory=datetime.now)
    source_message_id: Optional[str] = None

    @property
    def actor_key(self) -> str:
        return f"{self.channel}:{self.external_user_id}"


@dataclass(frozen=True)
class DlqActorEvent:
    event: InboundActorEvent
    reason: str
    error_message: str = ""


@dataclass(frozen=True)
class StreamEnvelope:
    message_id: str
    event: InboundActorEvent


class RedisStreamBus:
    def __init__(
        self,
        *,
        redis_url: str,
        redis_password: str = "",
        inbound_stream: str = DEFAULT_INBOUND_STREAM,
        dlq_stream: str = DEFAULT_DLQ_STREAM,
        consumer_group: str = DEFAULT_CONSUMER_GROUP,
    ) -> None:
        self._redis_url = str(redis_url or "").strip()
        self._redis_password = str(redis_password or "").strip()
        self._inbound_stream = inbound_stream
        self._dlq_stream = dlq_stream
        self._consumer_group = consumer_group
        self._client = None

    @property
    def inbound_stream(self) -> str:
        return self._inbound_stream

    @property
    def consumer_group(self) -> str:
        return self._consumer_group

    async def connect(self) -> None:
        if self._client is not None:
            return

        Redis, _ = _load_redis_modules()
        self._client = Redis.from_url(
            self._redis_url,
            password=self._redis_password or None,
            decode_responses=True,
        )

    async def close(self) -> None:
        if self._client is None:
            return
        await self._client.close()
        self._client = None

    async def ensure_consumer_group(self) -> None:
        await self.connect()
        _, ResponseError = _load_redis_modules()
        try:
            await self._client.xgroup_create(
                name=self._inbound_stream,
                groupname=self._consumer_group,
                id="0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def publish_inbound_event(self, event: InboundActorEvent) -> str:
        await self.connect()
        return await self._client.xadd(self._inbound_stream, self._serialize_event(event))

    async def publish_dlq_event(self, dlq_event: DlqActorEvent) -> str:
        await self.connect()
        payload = {
            **self._serialize_event(dlq_event.event),
            "reason": dlq_event.reason,
            "error_message": dlq_event.error_message,
        }
        return await self._client.xadd(self._dlq_stream, payload)

    async def read_consumer_group(
        self,
        *,
        consumer_name: str,
        count: int = 10,
        block_ms: int = 1000,
    ) -> List[StreamEnvelope]:
        await self.ensure_consumer_group()
        response = await self._client.xreadgroup(
            groupname=self._consumer_group,
            consumername=consumer_name,
            streams={self._inbound_stream: ">"},
            count=count,
            block=block_ms,
        )

        messages: List[StreamEnvelope] = []
        for _, items in response or []:
            for message_id, fields in items:
                messages.append(StreamEnvelope(message_id=message_id, event=self._deserialize_event(message_id, fields)))
        return messages

    async def reclaim_pending(
        self,
        *,
        consumer_name: str,
        min_idle_ms: int = 1000,
        count: int = 10,
        start_id: str = "0-0",
    ) -> List[StreamEnvelope]:
        await self.ensure_consumer_group()
        response = await self._client.xautoclaim(
            name=self._inbound_stream,
            groupname=self._consumer_group,
            consumername=consumer_name,
            min_idle_time=max(0, int(min_idle_ms)),
            start_id=start_id,
            count=count,
        )

        messages: List[StreamEnvelope] = []
        claimed_items = []
        if isinstance(response, (list, tuple)) and len(response) >= 2:
            claimed_items = response[1] or []

        for message_id, fields in claimed_items:
            messages.append(StreamEnvelope(message_id=message_id, event=self._deserialize_event(message_id, fields)))
        return messages

    async def ack(self, *message_ids: str) -> int:
        if not message_ids:
            return 0
        await self.connect()
        return await self._client.xack(self._inbound_stream, self._consumer_group, *message_ids)

    @staticmethod
    def _serialize_event(event: InboundActorEvent) -> Dict[str, str]:
        return {
            "event_id": event.event_id,
            "channel": event.channel,
            "external_user_id": event.external_user_id,
            "payload": json.dumps(event.payload, ensure_ascii=False),
            "attempt": str(int(event.attempt)),
            "occurred_at": event.occurred_at.isoformat(),
        }

    @staticmethod
    def _deserialize_event(message_id: str, fields: Dict[str, Any]) -> InboundActorEvent:
        raw_payload = fields.get("payload")
        payload: Dict[str, object] = {}
        if isinstance(raw_payload, str) and raw_payload.strip():
            try:
                decoded = json.loads(raw_payload)
                if isinstance(decoded, dict):
                    payload = decoded
            except json.JSONDecodeError:
                logger.warning("Invalid inbound actor payload JSON: %s", raw_payload)

        occurred_at_raw = str(fields.get("occurred_at") or "").strip()
        occurred_at = datetime.now()
        if occurred_at_raw:
            try:
                occurred_at = datetime.fromisoformat(occurred_at_raw)
            except ValueError:
                logger.warning("Invalid inbound actor occurred_at=%s", occurred_at_raw)

        event_id = str(fields.get("event_id") or "").strip() or f"stream:{message_id}"
        return InboundActorEvent(
            event_id=event_id,
            channel=str(fields.get("channel") or "").strip() or "wecom",
            external_user_id=str(fields.get("external_user_id") or "").strip(),
            payload=payload,
            attempt=int(fields.get("attempt") or 0),
            occurred_at=occurred_at,
            source_message_id=message_id,
        )
