from app.config import Settings


def test_settings_accepts_empty_zhipu_web_search_enabled(monkeypatch):
    monkeypatch.setenv("ZHIPU_WEB_SEARCH_ENABLED", "")

    current = Settings()

    assert current.zhipu_web_search_enabled is True


def test_settings_builds_postgres_database_url(monkeypatch):
    monkeypatch.setenv("DATABASE_TYPE", "postgres")
    monkeypatch.setenv("POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_USER", "demo")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_DB", "lovagent")

    current = Settings()

    assert current.database_url == "postgresql://demo:secret@127.0.0.1:5432/lovagent"


def test_settings_keeps_sqlite_and_mysql_database_urls(monkeypatch):
    monkeypatch.setenv("DATABASE_TYPE", "sqlite")
    monkeypatch.setenv("DATABASE_PATH", "./app.db")

    assert Settings().database_url == "sqlite:///./app.db"

    monkeypatch.setenv("DATABASE_TYPE", "mysql")
    monkeypatch.setenv("MYSQL_HOST", "db.local")
    monkeypatch.setenv("MYSQL_PORT", "3307")
    monkeypatch.setenv("MYSQL_USER", "root")
    monkeypatch.setenv("MYSQL_PASSWORD", "pw")
    monkeypatch.setenv("MYSQL_DATABASE", "girlchat")

    assert Settings().database_url == "mysql+pymysql://root:pw@db.local:3307/girlchat"
