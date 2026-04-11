# Database Type Postgres Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `DATABASE_TYPE=postgres` support so the app builds a PostgreSQL connection URL from `POSTGRES_*` env vars while preserving sqlite and mysql compatibility.

**Architecture:** Keep `app/config.py` as the single source of truth for database connection configuration. Extend `Settings` with PostgreSQL fields and branch `database_url` explicitly on `sqlite`, `postgres`, and `mysql` so the rest of the app can continue consuming `settings.database_url` unchanged.

**Tech Stack:** Python, Pydantic Settings, SQLAlchemy, pytest

---

### Task 1: Add config regression tests

**Files:**
- Modify: `tests/test_config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

```python
def test_settings_builds_postgres_database_url(monkeypatch):
    monkeypatch.setenv("DATABASE_TYPE", "postgres")
    monkeypatch.setenv("POSTGRES_HOST", "127.0.0.1")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_USER", "demo")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_DB", "lovagent")

    current = Settings()

    assert current.database_url == "postgresql://demo:secret@127.0.0.1:5432/lovagent"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_config.py -q`
Expected: FAIL because `Settings` does not yet expose PostgreSQL settings or generate a PostgreSQL URL.

- [ ] **Step 3: Write minimal implementation**

```python
postgres_host: str = os.getenv("POSTGRES_HOST", "localhost")
postgres_port: int = int(os.getenv("POSTGRES_PORT", "5432"))
postgres_user: str = os.getenv("POSTGRES_USER", "postgres")
postgres_password: str = os.getenv("POSTGRES_PASSWORD", "")
postgres_db: str = os.getenv("POSTGRES_DB", "postgres")

if self.database_type == "sqlite":
    return f"sqlite:///{self.database_path}"
if self.database_type == "postgres":
    return (
        f"postgresql://{self.postgres_user}:{self.postgres_password}"
        f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    )
return f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_config.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_config.py app/config.py docs/superpowers/plans/2026-04-12-database-type-postgres.md
git commit -m "feat(config): add postgres database type support"
```

### Task 2: Preserve compatibility behavior

**Files:**
- Modify: `tests/test_config.py`
- Modify: `app/config.py`
- Test: `tests/test_config.py`

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python -m pytest tests/test_config.py -q`
Expected: FAIL if the new branching regresses existing sqlite or mysql behavior.

- [ ] **Step 3: Write minimal implementation**

```python
database_type = self.database_type.strip().lower()
if database_type == "sqlite":
    ...
if database_type == "postgres":
    ...
return mysql_url
```

- [ ] **Step 4: Run test to verify it passes**

Run: `python -m pytest tests/test_config.py -q`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add tests/test_config.py app/config.py
git commit -m "test(config): cover database type url branches"
```
