"""
Actor persistence models.
"""

from datetime import datetime

from sqlalchemy import Column, DateTime, Integer, String, UniqueConstraint

from app.models.user import Base


class ActorInflightState(Base):
    __tablename__ = "actor_inflight_state"
    __table_args__ = (
        UniqueConstraint("channel", "external_user_id", name="uq_actor_inflight_state_channel_external_user_id"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    channel = Column(String(32), nullable=False, comment="Channel name")
    external_user_id = Column(String(128), nullable=False, comment="Channel user id")
    generation_version = Column(Integer, nullable=False, default=0, comment="Generation version")
    status = Column(String(32), nullable=False, default="collecting", comment="Inflight status")
    buffer_count = Column(Integer, nullable=False, default=0, comment="Buffered event count")
    last_event_at = Column(DateTime, nullable=True, comment="Last inbound event time")
    updated_at = Column(DateTime, nullable=False, default=datetime.now, onupdate=datetime.now, comment="Updated at")

    def __repr__(self):
        return (
            f"<ActorInflightState(id={self.id}, channel={self.channel}, "
            f"external_user_id={self.external_user_id}, status={self.status})>"
        )


class InboundEventDedup(Base):
    __tablename__ = "inbound_event_dedup"
    __table_args__ = (
        UniqueConstraint("event_id", "actor_key", name="uq_inbound_event_dedup_event_id_actor_key"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(128), nullable=False, comment="Inbound event id")
    actor_key = Column(String(160), nullable=False, comment="Actor identity key")
    processed_at = Column(DateTime, nullable=True, default=None, comment="Processed at")

    def __repr__(self):
        return f"<InboundEventDedup(id={self.id}, event_id={self.event_id}, actor_key={self.actor_key})>"
