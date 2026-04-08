import unittest
from datetime import datetime

from sqlalchemy.exc import IntegrityError

from app.models.actor import ActorInflightState, InboundEventDedup
from app.models.database import SessionLocal, init_db


class ActorModelsTests(unittest.TestCase):
    def setUp(self):
        init_db()
        self.db = SessionLocal()
        self.db.query(InboundEventDedup).delete()
        self.db.query(ActorInflightState).delete()
        self.db.commit()

    def tearDown(self):
        self.db.rollback()
        self.db.query(InboundEventDedup).delete()
        self.db.query(ActorInflightState).delete()
        self.db.commit()
        self.db.close()

    def test_actor_inflight_state_insert_persists_defaults(self):
        state = ActorInflightState(
            channel="wecom",
            external_user_id="user-1",
            last_event_at=datetime(2026, 4, 9, 12, 0, 0),
        )

        self.db.add(state)
        self.db.commit()

        stored = (
            self.db.query(ActorInflightState)
            .filter(
                ActorInflightState.channel == "wecom",
                ActorInflightState.external_user_id == "user-1",
            )
            .one()
        )

        self.assertIsNotNone(stored.id)
        self.assertEqual(stored.generation_version, 0)
        self.assertEqual(stored.status, "collecting")
        self.assertEqual(stored.buffer_count, 0)
        self.assertEqual(stored.last_event_at, datetime(2026, 4, 9, 12, 0, 0))
        self.assertIsNotNone(stored.updated_at)

    def test_inbound_event_dedup_enforces_event_actor_uniqueness(self):
        first = InboundEventDedup(event_id="evt-1", actor_key="wecom:user-1")
        duplicate = InboundEventDedup(event_id="evt-1", actor_key="wecom:user-1")

        self.db.add(first)
        self.db.commit()

        self.db.add(duplicate)
        with self.assertRaises(IntegrityError):
            self.db.commit()

        self.db.rollback()

        distinct_actor = InboundEventDedup(event_id="evt-1", actor_key="wecom:user-2")
        self.db.add(distinct_actor)
        self.db.commit()

        self.assertEqual(self.db.query(InboundEventDedup).count(), 2)


if __name__ == "__main__":
    unittest.main()
