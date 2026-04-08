from copy import deepcopy
import unittest
from unittest.mock import patch

from app.config import settings
from app.models.admin import RuntimeConfig
from app.models.database import SessionLocal, init_db
from app.services.runtime_config_service import RUNTIME_CONFIG_KEY, runtime_config_service


class RuntimeConfigServiceTests(unittest.TestCase):
    def setUp(self):
        init_db()
        self.db = SessionLocal()
        existing = (
            self.db.query(RuntimeConfig)
            .filter(RuntimeConfig.config_key == RUNTIME_CONFIG_KEY)
            .first()
        )
        self.runtime_snapshot = deepcopy(existing.config_value) if existing else None

    def tearDown(self):
        record = (
            self.db.query(RuntimeConfig)
            .filter(RuntimeConfig.config_key == RUNTIME_CONFIG_KEY)
            .first()
        )
        if self.runtime_snapshot is None:
            if record:
                self.db.delete(record)
        else:
            if not record:
                record = RuntimeConfig(config_key=RUNTIME_CONFIG_KEY)
                self.db.add(record)
            record.config_value = deepcopy(self.runtime_snapshot)

        self.db.commit()
        runtime_config_service.invalidate_cache()
        self.db.close()

    def _clear_runtime_config(self):
        record = (
            self.db.query(RuntimeConfig)
            .filter(RuntimeConfig.config_key == RUNTIME_CONFIG_KEY)
            .first()
        )
        if record:
            self.db.delete(record)
            self.db.commit()
        runtime_config_service.invalidate_cache()

    def test_effective_config_prefers_runtime_values_and_falls_back_to_env(self):
        self._clear_runtime_config()

        with (
            patch.object(settings, "model_provider", "glm"),
            patch.object(settings, "zhipu_api_key", "env-key"),
            patch.object(settings, "zhipu_model", "glm-env"),
            patch.object(settings, "zhipu_multimodal_api_key", ""),
            patch.object(settings, "zhipu_multimodal_model", "glm-4.6v"),
            patch.object(settings, "openai_api_key", "env-openai-key"),
            patch.object(settings, "openai_base_url", "https://env-openai.example.com/v1"),
            patch.object(settings, "openai_model", "env-openai-model"),
            patch.object(settings, "wecom_corp_id", "env-corp"),
            patch.object(settings, "wecom_agent_id", "env-agent"),
            patch.object(settings, "wecom_secret", "env-secret"),
            patch.object(settings, "wecom_token", "env-token"),
            patch.object(settings, "wecom_encoding_aes_key", "env-aes"),
            patch.object(settings, "public_base_url", "https://env.example.com"),
            patch.object(settings, "admin_password", "env-admin"),
        ):
            runtime_config_service.save_section("model", {"zhipu_model": "glm-5"})
            runtime_config_service.save_section("wecom", {"corp_id": "runtime-corp", "agent_id": "runtime-agent"})

            effective_model = runtime_config_service.get_effective_model_config()
            effective_wecom = runtime_config_service.get_effective_wecom_config()

            self.assertEqual(effective_model["zhipu_api_key"], "env-key")
            self.assertEqual(effective_model["zhipu_model"], "glm-5")
            self.assertEqual(effective_model["multimodal_api_key"], "env-key")
            self.assertEqual(effective_model["multimodal_model"], "glm-4.6v")
            self.assertEqual(effective_model["openai_model"], "env-openai-model")
            self.assertEqual(effective_wecom["corp_id"], "runtime-corp")
            self.assertEqual(effective_wecom["agent_id"], "runtime-agent")
            self.assertEqual(effective_wecom["secret"], "env-secret")
            self.assertEqual(runtime_config_service.get_effective_public_base_url(), "https://env.example.com")
            self.assertEqual(runtime_config_service.get_effective_admin_password(), "env-admin")

    def test_status_payload_reports_completion_and_callback_url(self):
        self._clear_runtime_config()
        with patch.object(settings, "model_provider", "glm"):
            runtime_config_service.save_section(
                "model",
                {
                    "zhipu_api_key": "test-key",
                    "zhipu_model": "glm-5",
                    "zhipu_thinking_type": "disabled",
                },
            )
            runtime_config_service.save_section(
                "wecom",
                {
                    "corp_id": "ww-test",
                    "agent_id": "1000002",
                    "secret": "secret-test",
                    "token": "token-test",
                    "encoding_aes_key": "encoding-test",
                },
            )
            runtime_config_service.save_section("deployment", {"public_base_url": "https://demo.trycloudflare.com"})
            runtime_config_service.save_section("admin", {"password": "secret123"})

            payload = runtime_config_service.get_status_payload()

            self.assertTrue(payload["setup_completed"])
            self.assertTrue(payload["sections"]["deployment_configured"])
            self.assertEqual(payload["current"]["model_provider"], "glm")
            self.assertEqual(payload["current"]["callback_url"], "https://demo.trycloudflare.com/wecom/callback")
            self.assertEqual(payload["raw"]["deployment"]["public_base_url"], "https://demo.trycloudflare.com")

    def test_openai_auto_model_config_uses_routed_models(self):
        self._clear_runtime_config()

        with (
            patch.object(settings, "model_provider", "glm"),
            patch.object(settings, "openai_api_key", ""),
            patch.object(settings, "openai_base_url", "https://api.openai.com/v1"),
            patch.object(settings, "openai_model", "fallback-model"),
            patch.object(settings, "zhipu_multimodal_api_key", ""),
            patch.object(settings, "zhipu_multimodal_model", "glm-4.6v"),
        ):
            runtime_config_service.save_section(
                "model",
                {
                    "model_provider": "openai_compatible",
                    "openai_api_key": "openai-key",
                    "openai_base_url": "https://openrouter.example.com/v1",
                    "multimodal_api_key": "mm-key",
                    "multimodal_model": "glm-4.6v",
                    "openai_model_mode": "auto",
                    "openai_models": {
                        "chat_model": "chat-x",
                        "memory_model": "memory-x",
                        "proactive_model": "proactive-x",
                    },
                },
            )

            effective_model = runtime_config_service.get_effective_model_config()
            self.assertEqual(effective_model["model_provider"], "openai_compatible")
            self.assertEqual(effective_model["openai_model_mode"], "auto")
            self.assertEqual(effective_model["openai_models"]["chat_model"], "chat-x")
            self.assertEqual(effective_model["openai_models"]["memory_model"], "memory-x")
            self.assertEqual(effective_model["openai_models"]["proactive_model"], "proactive-x")
            self.assertEqual(effective_model["multimodal_api_key"], "mm-key")
            self.assertEqual(effective_model["multimodal_model"], "glm-4.6v")
            self.assertTrue(runtime_config_service.is_model_configured())
            self.assertTrue(runtime_config_service.is_multimodal_configured())

            payload = runtime_config_service.get_status_payload()
            self.assertEqual(payload["current"]["model_provider"], "openai_compatible")
            self.assertEqual(payload["current"]["openai_model_mode"], "auto")
            self.assertEqual(payload["current"]["openai_models"]["chat_model"], "chat-x")
            self.assertEqual(payload["current"]["multimodal_model"], "glm-4.6v")
            self.assertTrue(payload["current"]["has_multimodal_api_key"])
            self.assertTrue(payload["current"]["multimodal_configured"])
            self.assertTrue(payload["current"]["has_openai_api_key"])

    def test_provider_id_takes_precedence_when_model_provider_not_sent(self):
        self._clear_runtime_config()

        with (
            patch.object(settings, "model_provider", "glm"),
            patch.object(settings, "openai_api_key", "env-openai-key"),
            patch.object(settings, "openai_base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
        ):
            runtime_config_service.save_section(
                "model",
                {
                    "provider_id": "qwen",
                    "openai_api_key": "qwen-key",
                    "openai_base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                    "openai_model": "qwen-plus",
                },
            )

            raw_model = runtime_config_service.get_config()["model"]
            effective_model = runtime_config_service.get_effective_model_config()

            self.assertEqual(raw_model["provider_id"], "qwen")
            self.assertEqual(effective_model["provider_id"], "qwen")
            self.assertEqual(effective_model["model_provider"], "openai_compatible")
            self.assertEqual(effective_model["provider_base_url"], "https://dashscope.aliyuncs.com/compatible-mode/v1")

    def test_env_model_provider_sets_default_provider_when_runtime_config_missing(self):
        self._clear_runtime_config()

        with (
            patch.object(settings, "model_provider", "openai_compatible"),
            patch.object(settings, "openai_api_key", "env-openai-key"),
            patch.object(settings, "openai_base_url", "https://api.openai.com/v1"),
            patch.object(settings, "openai_model", "gpt-4o-mini"),
        ):
            raw_model = runtime_config_service.get_config()["model"]
            effective_model = runtime_config_service.get_effective_model_config()

            self.assertEqual(raw_model["provider_id"], "openai")
            self.assertEqual(raw_model["model_provider"], "openai")
            self.assertEqual(effective_model["provider_id"], "openai")
            self.assertEqual(effective_model["model_provider"], "openai_compatible")
            self.assertEqual(effective_model["openai_api_key"], "env-openai-key")

    def test_empty_model_section_keeps_env_derived_glm_default_despite_openai_base_url(self):
        self._clear_runtime_config()

        with (
            patch.object(settings, "model_provider", "glm"),
            patch.object(settings, "openai_base_url", "https://api.openai.com/v1"),
            patch.object(settings, "openai_model", "gpt-4o-mini"),
        ):
            runtime_record = RuntimeConfig(
                config_key=RUNTIME_CONFIG_KEY,
                config_value={"model": {}},
            )
            self.db.add(runtime_record)
            self.db.commit()
            runtime_config_service.invalidate_cache()

            raw_model = runtime_config_service.get_config()["model"]
            effective_model = runtime_config_service.get_effective_model_config()

            self.assertEqual(raw_model["provider_id"], "zhipu")
            self.assertEqual(raw_model["model_provider"], "glm")
            self.assertEqual(effective_model["provider_id"], "zhipu")
            self.assertEqual(effective_model["model_provider"], "glm")

    def test_effective_actor_config_prefers_runtime_values_and_normalizes_bounds(self):
        self._clear_runtime_config()

        with (
            patch.object(settings, "redis_url", "redis://env.example.com:6379/0"),
            patch.object(settings, "redis_password", "env-secret"),
            patch.object(settings, "actor_pipeline_enabled", True),
            patch.object(settings, "actor_debounce_ms", 2400),
            patch.object(settings, "actor_max_messages_per_turn", 10),
            patch.object(settings, "actor_first_reply_delay_ms", 300),
            patch.object(settings, "actor_chunk_delay_ms", 200),
            patch.object(settings, "actor_reply_chunk_min", 1),
            patch.object(settings, "actor_reply_chunk_max", 5),
            patch.object(settings, "actor_retry_max_attempts", 3),
            patch.object(settings, "actor_retry_backoff_base_ms", 300),
        ):
            runtime_config_service.save_section(
                "channels_actor",
                {
                    "actor_pipeline_enabled": False,
                    "actor_debounce_ms": -50,
                    "actor_max_messages_per_turn": 0,
                    "actor_first_reply_delay_ms": -120,
                    "actor_chunk_delay_ms": -80,
                    "actor_reply_chunk_min": 0,
                    "actor_reply_chunk_max": 0,
                    "actor_retry_max_attempts": -2,
                    "actor_retry_backoff_base_ms": -150,
                },
            )

            normalized_actor = runtime_config_service.get_config()["channels_actor"]
            effective_actor = runtime_config_service.get_effective_actor_config()

            self.assertEqual(effective_actor["redis_url"], "redis://env.example.com:6379/0")
            self.assertEqual(effective_actor["redis_password"], "env-secret")
            self.assertFalse(effective_actor["actor_pipeline_enabled"])
            self.assertEqual(effective_actor["actor_debounce_ms"], 0)
            self.assertEqual(effective_actor["actor_max_messages_per_turn"], 1)
            self.assertEqual(effective_actor["actor_first_reply_delay_ms"], 0)
            self.assertEqual(effective_actor["actor_chunk_delay_ms"], 0)
            self.assertEqual(effective_actor["actor_reply_chunk_min"], 1)
            self.assertEqual(effective_actor["actor_reply_chunk_max"], 1)
            self.assertEqual(effective_actor["actor_retry_max_attempts"], 0)
            self.assertEqual(effective_actor["actor_retry_backoff_base_ms"], 0)
            self.assertEqual(normalized_actor["actor_first_reply_delay_ms"], 0)
            self.assertEqual(normalized_actor["actor_chunk_delay_ms"], 0)
            self.assertEqual(normalized_actor["actor_retry_backoff_base_ms"], 0)

    def test_actor_config_uses_env_defaults_when_runtime_values_missing(self):
        self._clear_runtime_config()

        with (
            patch.object(settings, "redis_url", "redis://127.0.0.1:6379/1"),
            patch.object(settings, "redis_password", "runtime-fallback"),
            patch.object(settings, "actor_pipeline_enabled", False),
            patch.object(settings, "actor_debounce_ms", 2600),
            patch.object(settings, "actor_max_messages_per_turn", 12),
            patch.object(settings, "actor_first_reply_delay_ms", 320),
            patch.object(settings, "actor_chunk_delay_ms", 180),
            patch.object(settings, "actor_reply_chunk_min", 2),
            patch.object(settings, "actor_reply_chunk_max", 6),
            patch.object(settings, "actor_retry_max_attempts", 4),
            patch.object(settings, "actor_retry_backoff_base_ms", 450),
        ):
            effective_actor = runtime_config_service.get_effective_actor_config()

            self.assertEqual(
                effective_actor,
                {
                    "redis_url": "redis://127.0.0.1:6379/1",
                    "redis_password": "runtime-fallback",
                    "actor_pipeline_enabled": False,
                    "actor_debounce_ms": 2600,
                    "actor_max_messages_per_turn": 12,
                    "actor_first_reply_delay_ms": 320,
                    "actor_chunk_delay_ms": 180,
                    "actor_reply_chunk_min": 2,
                    "actor_reply_chunk_max": 6,
                    "actor_retry_max_attempts": 4,
                    "actor_retry_backoff_base_ms": 450,
                },
            )

    def test_actor_config_clamps_negative_env_defaults_for_timing_fields(self):
        self._clear_runtime_config()

        with (
            patch.object(settings, "actor_debounce_ms", -2600),
            patch.object(settings, "actor_first_reply_delay_ms", -320),
            patch.object(settings, "actor_chunk_delay_ms", -180),
            patch.object(settings, "actor_retry_backoff_base_ms", -450),
        ):
            effective_actor = runtime_config_service.get_effective_actor_config()

            self.assertEqual(effective_actor["actor_debounce_ms"], 0)
            self.assertEqual(effective_actor["actor_first_reply_delay_ms"], 0)
            self.assertEqual(effective_actor["actor_chunk_delay_ms"], 0)
            self.assertEqual(effective_actor["actor_retry_backoff_base_ms"], 0)

    def test_actor_settings_payload_hides_password_and_reports_presence(self):
        self._clear_runtime_config()

        with (
            patch.object(settings, "redis_url", "redis://env.example.com:6379/0"),
            patch.object(settings, "redis_password", ""),
        ):
            runtime_config_service.save_section(
                "channels_actor",
                {
                    "redis_url": "redis://runtime.example.com:6379/2",
                    "redis_password": "runtime-secret",
                    "actor_pipeline_enabled": True,
                },
            )

            effective_actor = runtime_config_service.get_effective_actor_config()
            public_actor = runtime_config_service.get_actor_settings_payload()

            self.assertEqual(effective_actor["redis_password"], "runtime-secret")
            self.assertNotIn("redis_password", public_actor)
            self.assertTrue(public_actor["has_redis_password"])
            self.assertEqual(public_actor["redis_url"], "redis://runtime.example.com:6379/2")

    def test_actor_settings_payload_strips_userinfo_from_redis_url(self):
        self._clear_runtime_config()

        cases = [
            ("redis://:secret@host.example.com:6379/0", "redis://host.example.com:6379/0"),
            ("redis://token@host.example.com:6379/0", "redis://host.example.com:6379/0"),
            ("redis://user:secret@host.example.com:6379/0", "redis://host.example.com:6379/0"),
        ]

        for redis_url, expected in cases:
            with self.subTest(redis_url=redis_url):
                runtime_config_service.save_section(
                    "channels_actor",
                    {
                        "redis_url": redis_url,
                        "redis_password": "runtime-secret",
                    },
                )

                public_actor = runtime_config_service.get_actor_settings_payload()

                self.assertEqual(public_actor["redis_url"], expected)
                self.assertNotIn("@", public_actor["redis_url"])
                self.assertNotIn("secret", public_actor["redis_url"])
                self.assertNotIn("token", public_actor["redis_url"])
                self.assertNotIn("user", public_actor["redis_url"])


if __name__ == "__main__":
    unittest.main()
