from unittest.mock import patch

from app.models.database import _describe_incompatible_users_table, init_db


class FakeInspector:
    def __init__(self, tables, columns_by_table):
        self._tables = tables
        self._columns_by_table = columns_by_table

    def get_table_names(self):
        return self._tables

    def get_columns(self, table_name):
        return self._columns_by_table[table_name]


def test_describe_incompatible_users_table_detects_legacy_public_users_shape():
    inspector = FakeInspector(
        tables=["users"],
        columns_by_table={
            "users": [
                {"name": "id", "type": "TEXT"},
                {"name": "username", "type": "TEXT"},
                {"name": "password_hash", "type": "TEXT"},
                {"name": "role", "type": "TEXT"},
            ]
        },
    )

    detail = _describe_incompatible_users_table(inspector)

    assert detail is not None
    assert "users.id" in detail
    assert "text" in detail.lower()
    assert "username" in detail
    assert "password_hash" in detail


def test_describe_incompatible_users_table_accepts_current_users_shape():
    inspector = FakeInspector(
        tables=["users"],
        columns_by_table={
            "users": [
                {"name": "id", "type": "INTEGER"},
                {"name": "channel", "type": "VARCHAR"},
                {"name": "external_user_id", "type": "VARCHAR"},
            ]
        },
    )

    assert _describe_incompatible_users_table(inspector) is None


def test_init_db_raises_clear_error_for_incompatible_users_table():
    inspector = FakeInspector(
        tables=["users"],
        columns_by_table={
            "users": [
                {"name": "id", "type": "TEXT"},
                {"name": "username", "type": "TEXT"},
                {"name": "password_hash", "type": "TEXT"},
            ]
        },
    )

    with patch("app.models.database.inspect", return_value=inspector):
        try:
            init_db()
        except RuntimeError as exc:
            message = str(exc)
        else:
            raise AssertionError("init_db() should raise for an incompatible users table")

    assert "dedicated POSTGRES_DB" in message
    assert "users.id is text" in message.lower()
