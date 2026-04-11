"""
Delivery executors.
"""

from __future__ import annotations

from datetime import datetime
import logging
from typing import Dict, Optional

from app.models.admin import ProactiveChatLog
from app.models.database import SessionLocal
from app.models.user import Conversation, User
from app.services.channel_dispatcher import channel_dispatcher
from app.services.llm_service import glm_service
from app.services.runtime_config_service import runtime_config_service

logger = logging.getLogger(__name__)


async def deliver_incoming_reply(*, channel: str, external_user_id: str, content: str) -> Dict[str, object]:
    actor_config = runtime_config_service.get_effective_actor_config()
    envelope = glm_service.parse_reply_envelope(
        content,
        chunk_min=int(actor_config["actor_reply_chunk_min"]),
        chunk_max=int(actor_config["actor_reply_chunk_max"]),
    )
    if envelope is None:
        return {"attempted": True, "status": "failed", "error_message": "structured_reply_parse_failed"}
    logger.debug(
        "Structured incoming reply metadata: tone=%s reason=%s chunks=%s",
        envelope.tone,
        envelope.reason,
        len(envelope.chunks),
    )
    delivery_result: Dict[str, object] = {"attempted": True, "status": "sent"}
    try:
        await channel_dispatcher.send_text_chunks(channel, external_user_id, envelope.chunks)
    except Exception as exc:
        logger.warning("Send message failed: %s", exc)
        delivery_result = {"attempted": True, "status": "failed", "error_message": str(exc)}
    return delivery_result


async def deliver_proactive_outreach(
    *,
    target_channel: str,
    target_external_user_id: str,
    trigger_type: str,
    window_key: Optional[str],
    content: str,
) -> Dict[str, object]:
    actor_config = runtime_config_service.get_effective_actor_config()
    envelope = glm_service.parse_reply_envelope(
        content,
        chunk_min=int(actor_config["actor_reply_chunk_min"]),
        chunk_max=int(actor_config["actor_reply_chunk_max"]),
    )
    if envelope is None:
        return {
            "attempted": True,
            "status": "failed",
            "error_message": "structured_reply_parse_failed",
            "sent_at": datetime.now().isoformat(),
        }
    logger.debug(
        "Structured proactive reply metadata: tone=%s reason=%s chunks=%s",
        envelope.tone,
        envelope.reason,
        len(envelope.chunks),
    )
    rendered_content = glm_service.render_reply_envelope_text(envelope)
    sent_at = datetime.now()
    status = "failed"
    error_message = None

    try:
        await channel_dispatcher.send_text_chunks(target_channel, target_external_user_id, envelope.chunks)
        status = "sent"
        _save_proactive_conversation(
            target_channel=target_channel,
            target_external_user_id=target_external_user_id,
            content=rendered_content,
            sent_at=sent_at,
        )
    except Exception as exc:
        error_message = str(exc)
        logger.warning("Proactive delivery failed: %s", exc)
    finally:
        _save_proactive_log(
            target_channel=target_channel,
            target_external_user_id=target_external_user_id,
            trigger_type=trigger_type,
            window_key=window_key,
            content=rendered_content,
            status=status,
            error_message=error_message,
            sent_at=sent_at,
        )

    return {
        "attempted": True,
        "status": status,
        "error_message": error_message,
        "sent_at": sent_at.isoformat(),
    }


def _save_proactive_conversation(
    *,
    target_channel: str,
    target_external_user_id: str,
    content: str,
    sent_at: datetime,
) -> None:
    db = SessionLocal()
    try:
        user = (
            db.query(User)
            .filter(User.channel == target_channel, User.external_user_id == target_external_user_id)
            .first()
        )
        if not user:
            return

        conversation = Conversation(
            user_id=user.id,
            user_message="",
            agent_message=content,
            user_emotion=None,
            agent_emotion="happy",
            agent_emotion_intensity=40,
            context_used=True,
            memories_used={"source": "proactive"},
            created_at=sent_at,
        )
        db.add(conversation)
        db.commit()
    finally:
        db.close()


def _save_proactive_log(
    *,
    target_channel: str,
    target_external_user_id: str,
    trigger_type: str,
    window_key: Optional[str],
    content: str,
    status: str,
    error_message: Optional[str],
    sent_at: datetime,
) -> None:
    db = SessionLocal()
    try:
        log = ProactiveChatLog(
            target_channel=target_channel,
            target_external_user_id=target_external_user_id,
            trigger_type=trigger_type,
            window_key=window_key,
            content=content,
            status=status,
            error_message=error_message,
            sent_at=sent_at,
        )
        db.add(log)
        db.commit()
    finally:
        db.close()

