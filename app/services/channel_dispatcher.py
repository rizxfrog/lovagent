"""
Channel-aware outbound dispatcher.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Literal, Sequence

from app.services.wecom_service import wecom_service


GuardFn = Callable[[], bool | Awaitable[bool]]


@dataclass(frozen=True)
class ChunkDeliveryResult:
    channel: str
    status: Literal["sent", "cancelled", "not_sent"]
    sent_chunks: int


class ChannelDispatcher:
    async def send_text(self, channel: str, external_user_id: str, content: str) -> Dict[str, object]:
        lowered = (channel or "").strip().lower()
        if lowered == "wecom":
            await wecom_service.send_text_message(external_user_id, content)
            return {"channel": "wecom", "status": "sent"}

        if lowered == "napcat":
            from app.services.napcat_service import napcat_service

            await napcat_service.send_private_text(external_user_id, content)
            return {"channel": "napcat", "status": "sent"}

        raise ValueError(f"Unsupported channel: {channel}")

    async def send_text_chunks(
        self,
        channel: str,
        external_user_id: str,
        chunks: Sequence[str],
        *,
        first_delay_ms: int = 0,
        chunk_delay_ms: int = 0,
        should_continue: GuardFn | None = None,
    ) -> ChunkDeliveryResult:
        cleaned_chunks = [str(chunk).strip() for chunk in chunks if str(chunk).strip()]
        lowered = (channel or "").strip().lower()
        if not cleaned_chunks:
            return ChunkDeliveryResult(channel=lowered, status="not_sent", sent_chunks=0)

        sent_chunks = 0

        for index, chunk in enumerate(cleaned_chunks):
            delay_ms = first_delay_ms if index == 0 else chunk_delay_ms
            if delay_ms > 0:
                await asyncio.sleep(max(0, int(delay_ms)) / 1000.0)

            if not await self._should_continue(should_continue):
                return ChunkDeliveryResult(channel=lowered, status="cancelled", sent_chunks=sent_chunks)

            await self.send_text(lowered, external_user_id, chunk)
            sent_chunks += 1

        return ChunkDeliveryResult(channel=lowered, status="sent", sent_chunks=sent_chunks)

    async def _should_continue(self, guard: GuardFn | None) -> bool:
        if guard is None:
            return True
        result = guard()
        if isinstance(result, bool):
            return result
        return bool(await result)


channel_dispatcher = ChannelDispatcher()

