import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from app.services.napcat_service import NapCatService


class NapCatServiceTests(unittest.TestCase):
    def test_private_message_publishes_actor_event_when_pipeline_enabled(self):
        service = NapCatService()
        publish_mock = AsyncMock()
        graph_mock = AsyncMock()

        with (
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": True},
            ),
            patch(
                "app.services.inbound_actor_service.inbound_actor_service.publish_inbound_event",
                publish_mock,
                create=True,
            ),
            patch("app.graph.run_incoming_message_graph", graph_mock),
        ):
            asyncio.run(
                service._handle_message(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "user_id": "10001",
                            "raw_message": "hello from qq",
                        }
                    )
                )
            )

        publish_mock.assert_awaited_once()
        event = publish_mock.await_args.args[0]
        self.assertEqual(event.channel, "napcat")
        self.assertEqual(event.external_user_id, "10001")
        self.assertEqual(event.payload["text"], "hello from qq")
        graph_mock.assert_not_awaited()

    def test_private_message_runs_legacy_graph_when_pipeline_disabled(self):
        service = NapCatService()
        publish_mock = AsyncMock()
        graph_mock = AsyncMock(return_value={"agent_response": "ok"})

        with (
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": False},
            ),
            patch(
                "app.services.inbound_actor_service.inbound_actor_service.publish_inbound_event",
                publish_mock,
                create=True,
            ),
            patch("app.graph.run_incoming_message_graph", graph_mock),
        ):
            asyncio.run(
                service._handle_message(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "user_id": "10001",
                            "raw_message": "hello from qq",
                        }
                    )
                )
            )

        graph_mock.assert_awaited_once_with(
            {
                "channel": "napcat",
                "external_user_id": "10001",
                "user_content": "hello from qq",
            }
        )
        publish_mock.assert_not_awaited()

    def test_private_message_falls_back_to_legacy_graph_when_actor_publish_fails(self):
        service = NapCatService()
        graph_mock = AsyncMock(return_value={"agent_response": "ok"})

        with (
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": True},
            ),
            patch(
                "app.services.inbound_actor_service.inbound_actor_service.publish_inbound_event",
                AsyncMock(side_effect=RuntimeError("redis down")),
                create=True,
            ) as publish_mock,
            patch("app.graph.run_incoming_message_graph", graph_mock),
        ):
            asyncio.run(
                service._handle_message(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "user_id": "10001",
                            "raw_message": "hello from qq",
                        }
                    )
                )
            )

        publish_mock.assert_awaited_once()
        graph_mock.assert_awaited_once_with(
            {
                "channel": "napcat",
                "external_user_id": "10001",
                "user_content": "hello from qq",
            }
        )

    def test_private_image_message_publishes_actor_event_with_image_urls(self):
        service = NapCatService()
        publish_mock = AsyncMock()
        graph_mock = AsyncMock()

        with (
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": True},
            ),
            patch(
                "app.services.inbound_actor_service.inbound_actor_service.publish_inbound_event",
                publish_mock,
                create=True,
            ),
            patch("app.graph.run_incoming_message_graph", graph_mock),
        ):
            asyncio.run(
                service._handle_message(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "user_id": "10001",
                            "raw_message": "[CQ:image,file=abc]",
                            "message": [
                                {"type": "image", "data": {"url": "https://example.com/a.jpg"}},
                            ],
                        }
                    )
                )
            )

        publish_mock.assert_awaited_once()
        event = publish_mock.await_args.args[0]
        self.assertEqual(event.payload["text"], "[图片] 用户发来了一张图片")
        self.assertEqual(event.payload["image_urls"], ["https://example.com/a.jpg"])
        graph_mock.assert_not_awaited()

    def test_private_cq_image_message_extracts_url_from_raw_message(self):
        service = NapCatService()
        publish_mock = AsyncMock()

        with (
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": True},
            ),
            patch(
                "app.services.inbound_actor_service.inbound_actor_service.publish_inbound_event",
                publish_mock,
                create=True,
            ),
        ):
            asyncio.run(
                service._handle_message(
                    json.dumps(
                        {
                            "post_type": "message",
                            "message_type": "private",
                            "user_id": "10001",
                            "raw_message": "[CQ:image,file=x.png,url=https://host/a.png?x=1&amp;y=2]",
                        }
                    )
                )
            )

        publish_mock.assert_awaited_once()
        event = publish_mock.await_args.args[0]
        self.assertEqual(event.payload["image_urls"], ["https://host/a.png?x=1&y=2"])


if __name__ == "__main__":
    unittest.main()
