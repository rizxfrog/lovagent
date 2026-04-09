from app.config import Settings


def test_settings_accepts_empty_zhipu_web_search_enabled(monkeypatch):
    monkeypatch.setenv("ZHIPU_WEB_SEARCH_ENABLED", "")

    current = Settings()

    assert current.zhipu_web_search_enabled is True
