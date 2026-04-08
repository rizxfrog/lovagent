import asyncio
import unittest
import xml.etree.ElementTree as ET
from unittest.mock import AsyncMock, patch

try:
    from fastapi import HTTPException
    from app.routers import wecom
    from wechatpy.enterprise.crypto import WeChatCrypto
    IMPORT_ERROR = None
except ModuleNotFoundError as exc:  # pragma: no cover
    HTTPException = Exception
    wecom = None
    WeChatCrypto = None
    IMPORT_ERROR = exc


class DummyRequest:
    async def body(self):
        return b"<xml>encrypted</xml>"


@unittest.skipIf(wecom is None, f"missing dependency: {IMPORT_ERROR}")
class WeComCallbackVerifyTests(unittest.TestCase):
    def test_wecom_callback_verify_returns_plaintext(self):
        with patch.object(wecom.wecom_service, "verify_callback", return_value="decrypted-echo") as mocked_verify:
            response = asyncio.run(
                wecom.wecom_callback_verify(
                    msg_signature="sig",
                    timestamp="123",
                    nonce="nonce",
                    echostr="echo",
                )
            )

        mocked_verify.assert_called_once_with(
            msg_signature="sig",
            timestamp="123",
            nonce="nonce",
            echostr="echo",
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode(), "decrypted-echo")

    def test_wecom_callback_verify_requires_echostr(self):
        with self.assertRaises(HTTPException) as context:
            asyncio.run(
                wecom.wecom_callback_verify(
                    msg_signature="sig",
                    timestamp="123",
                    nonce="nonce",
                    echostr=None,
                )
            )

        self.assertEqual(context.exception.status_code, 400)
        self.assertEqual(context.exception.detail, "Missing echostr")

    def test_wecom_callback_verify_accepts_real_wecom_signature(self):
        config = {
            "corp_id": "ww2a6262fa052ccac3",
            "agent_id": "1000002",
            "secret": "secret-test",
            "token": "LoIsXTYpesDSdiGk7GS1nP7L7PAhTOy",
            "encoding_aes_key": "JuV3lxQO5mdnPQ7RmS0ocowz5CV2xoRuNmyd7NyEjjs",
        }
        crypto = WeChatCrypto(config["token"], config["encoding_aes_key"], config["corp_id"])
        encrypted_xml = crypto.encrypt_message("plain-echo", nonce="nonce-123", timestamp="1712476800")
        root = ET.fromstring(encrypted_xml)

        with patch(
            "app.services.wecom_service.runtime_config_service.get_effective_wecom_config",
            return_value=config,
        ):
            response = asyncio.run(
                wecom.wecom_callback_verify(
                    msg_signature=root.findtext("MsgSignature"),
                    timestamp=root.findtext("TimeStamp"),
                    nonce=root.findtext("Nonce"),
                    echostr=root.findtext("Encrypt"),
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode(), "plain-echo")


@unittest.skipIf(wecom is None, f"missing dependency: {IMPORT_ERROR}")
class WeComCallbackHandlerTests(unittest.TestCase):
    def test_handler_publishes_actor_event_when_pipeline_enabled(self):
        publish_mock = AsyncMock()
        register_mock = AsyncMock()
        with (
            patch.object(wecom.wecom_service, "decrypt_message", return_value="<xml />"),
            patch.object(
                wecom.wecom_service,
                "parse_message",
                return_value={
                    "msg_id": "msg-1",
                    "msg_type": "text",
                    "from_user": "user-1",
                    "content": "today feels heavy",
                },
            ),
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": True},
            ),
            patch(
                "app.services.inbound_actor_service.inbound_actor_service.publish_inbound_event",
                publish_mock,
                create=True,
            ),
            patch.object(wecom.incoming_aggregation_service, "register_event", register_mock),
            patch.object(wecom.incoming_aggregation_service, "schedule_user_processing") as schedule_mock,
        ):
            response = asyncio.run(
                wecom.wecom_callback_handler(
                    request=DummyRequest(),
                    msg_signature="sig",
                    timestamp="123",
                    nonce="nonce",
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode(), "success")
        publish_mock.assert_awaited_once()
        event = publish_mock.await_args.args[0]
        self.assertEqual(event.event_id, "msg-1")
        self.assertEqual(event.channel, "wecom")
        self.assertEqual(event.external_user_id, "user-1")
        self.assertEqual(event.payload["text"], "today feels heavy")
        register_mock.assert_not_awaited()
        schedule_mock.assert_not_called()

    def test_handler_uses_legacy_path_for_file_message_when_pipeline_enabled(self):
        publish_mock = AsyncMock()
        with (
            patch.object(wecom.wecom_service, "decrypt_message", return_value="<xml />"),
            patch.object(
                wecom.wecom_service,
                "parse_message",
                return_value={
                    "msg_id": "msg-file-1",
                    "msg_type": "file",
                    "from_user": "user-1",
                    "file_name": "report.pdf",
                    "media_id": "media-1",
                },
            ),
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": True},
            ),
            patch(
                "app.services.inbound_actor_service.inbound_actor_service.publish_inbound_event",
                publish_mock,
                create=True,
            ),
            patch.object(
                wecom.incoming_aggregation_service,
                "register_event",
                AsyncMock(return_value={"duplicate": False, "batch_id": 201, "user_id": "user-1"}),
            ) as register_mock,
            patch.object(wecom.incoming_aggregation_service, "schedule_user_processing") as schedule_mock,
        ):
            response = asyncio.run(
                wecom.wecom_callback_handler(
                    request=DummyRequest(),
                    msg_signature="sig",
                    timestamp="123",
                    nonce="nonce",
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode(), "success")
        publish_mock.assert_not_awaited()
        register_mock.assert_awaited_once()
        schedule_mock.assert_called_once_with("user-1")

    def test_handler_falls_back_to_legacy_path_when_actor_publish_fails(self):
        with (
            patch.object(wecom.wecom_service, "decrypt_message", return_value="<xml />"),
            patch.object(
                wecom.wecom_service,
                "parse_message",
                return_value={
                    "msg_id": "msg-2",
                    "msg_type": "text",
                    "from_user": "user-2",
                    "content": "fallback please",
                },
            ),
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": True},
            ),
            patch(
                "app.services.inbound_actor_service.inbound_actor_service.publish_inbound_event",
                AsyncMock(side_effect=RuntimeError("redis down")),
                create=True,
            ) as publish_mock,
            patch.object(
                wecom.incoming_aggregation_service,
                "register_event",
                AsyncMock(return_value={"duplicate": False, "batch_id": 202, "user_id": "user-2"}),
            ) as register_mock,
            patch.object(wecom.incoming_aggregation_service, "schedule_user_processing") as schedule_mock,
        ):
            response = asyncio.run(
                wecom.wecom_callback_handler(
                    request=DummyRequest(),
                    msg_signature="sig",
                    timestamp="123",
                    nonce="nonce",
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode(), "success")
        publish_mock.assert_awaited_once()
        register_mock.assert_awaited_once()
        schedule_mock.assert_called_once_with("user-2")

    def test_handler_registers_event_and_schedules_processing_when_pipeline_disabled(self):
        with (
            patch.object(wecom.wecom_service, "decrypt_message", return_value="<xml />"),
            patch.object(
                wecom.wecom_service,
                "parse_message",
                return_value={
                    "msg_id": "msg-1",
                    "msg_type": "text",
                    "from_user": "user-1",
                    "content": "today feels heavy",
                },
            ),
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": False},
            ),
            patch.object(
                wecom.incoming_aggregation_service,
                "register_event",
                AsyncMock(return_value={"duplicate": False, "batch_id": 101, "user_id": "user-1"}),
            ) as register_mock,
            patch.object(wecom.incoming_aggregation_service, "schedule_user_processing") as schedule_mock,
        ):
            response = asyncio.run(
                wecom.wecom_callback_handler(
                    request=DummyRequest(),
                    msg_signature="sig",
                    timestamp="123",
                    nonce="nonce",
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode(), "success")
        register_mock.assert_awaited_once()
        schedule_mock.assert_called_once_with("user-1")

    def test_handler_skips_scheduling_for_duplicate_event_when_pipeline_disabled(self):
        with (
            patch.object(wecom.wecom_service, "decrypt_message", return_value="<xml />"),
            patch.object(
                wecom.wecom_service,
                "parse_message",
                return_value={
                    "msg_id": "msg-1",
                    "msg_type": "text",
                    "from_user": "user-1",
                    "content": "today feels heavy",
                },
            ),
            patch(
                "app.services.runtime_config_service.runtime_config_service.get_effective_actor_config",
                return_value={"actor_pipeline_enabled": False},
            ),
            patch.object(
                wecom.incoming_aggregation_service,
                "register_event",
                AsyncMock(return_value={"duplicate": True, "batch_id": 101, "user_id": "user-1"}),
            ) as register_mock,
            patch.object(wecom.incoming_aggregation_service, "schedule_user_processing") as schedule_mock,
        ):
            response = asyncio.run(
                wecom.wecom_callback_handler(
                    request=DummyRequest(),
                    msg_signature="sig",
                    timestamp="123",
                    nonce="nonce",
                )
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.body.decode(), "success")
        register_mock.assert_awaited_once()
        schedule_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
