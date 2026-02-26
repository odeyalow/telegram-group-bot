from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from time import time
from uuid import uuid4


@dataclass(frozen=True)
class GroupSettings:
    chat_id: int
    title: str
    bot_enabled: bool
    moderator_trigger_enabled: bool
    anonymous_enabled: bool
    anonymous_token: str | None


_db_path = Path("bot.db")


def init_storage(db_path: str) -> None:
    global _db_path
    _db_path = Path(db_path)
    if _db_path.parent != Path(""):
        _db_path.parent.mkdir(parents=True, exist_ok=True)

    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS groups (
                chat_id INTEGER PRIMARY KEY,
                title TEXT NOT NULL DEFAULT '',
                bot_enabled INTEGER NOT NULL DEFAULT 1,
                moderator_trigger_enabled INTEGER NOT NULL DEFAULT 1,
                anonymous_enabled INTEGER NOT NULL DEFAULT 0,
                anonymous_token TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_anonymous_token
            ON groups(anonymous_token)
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meme_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                video_id TEXT NOT NULL,
                sent_at INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_meme_history_chat_time
            ON meme_history(chat_id, sent_at)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_meme_history_chat_video
            ON meme_history(chat_id, video_id)
            """
        )


def ensure_group(chat_id: int, title: str = "") -> GroupSettings:
    with _connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO groups (chat_id, title)
            VALUES (?, ?)
            """,
            (chat_id, title or ""),
        )
        if title:
            conn.execute(
                """
                UPDATE groups
                SET title = ?
                WHERE chat_id = ?
                """,
                (title, chat_id),
            )

    settings = get_group(chat_id)
    if settings is None:
        raise RuntimeError("Failed to initialize group settings")
    return settings


def get_group(chat_id: int) -> GroupSettings | None:
    with _connect(row_factory=True) as conn:
        row = conn.execute(
            """
            SELECT
                chat_id,
                title,
                bot_enabled,
                moderator_trigger_enabled,
                anonymous_enabled,
                anonymous_token
            FROM groups
            WHERE chat_id = ?
            """,
            (chat_id,),
        ).fetchone()
    return _row_to_settings(row) if row else None


def get_group_by_anonymous_token(token: str) -> GroupSettings | None:
    with _connect(row_factory=True) as conn:
        row = conn.execute(
            """
            SELECT
                chat_id,
                title,
                bot_enabled,
                moderator_trigger_enabled,
                anonymous_enabled,
                anonymous_token
            FROM groups
            WHERE anonymous_token = ?
            """,
            (token,),
        ).fetchone()
    return _row_to_settings(row) if row else None


def set_bot_enabled(chat_id: int, enabled: bool) -> None:
    ensure_group(chat_id)
    with _connect() as conn:
        conn.execute(
            """
            UPDATE groups
            SET bot_enabled = ?
            WHERE chat_id = ?
            """,
            (int(enabled), chat_id),
        )


def set_moderator_trigger_enabled(chat_id: int, enabled: bool) -> None:
    ensure_group(chat_id)
    with _connect() as conn:
        conn.execute(
            """
            UPDATE groups
            SET moderator_trigger_enabled = ?
            WHERE chat_id = ?
            """,
            (int(enabled), chat_id),
        )


def set_anonymous_enabled(chat_id: int, enabled: bool) -> None:
    ensure_group(chat_id)
    with _connect() as conn:
        conn.execute(
            """
            UPDATE groups
            SET anonymous_enabled = ?
            WHERE chat_id = ?
            """,
            (int(enabled), chat_id),
        )


def ensure_anonymous_token(chat_id: int) -> str:
    settings = ensure_group(chat_id)
    if settings.anonymous_token:
        return settings.anonymous_token

    for _ in range(10):
        candidate = uuid4().hex
        try:
            with _connect() as conn:
                conn.execute(
                    """
                    UPDATE groups
                    SET anonymous_token = ?
                    WHERE chat_id = ?
                    """,
                    (candidate, chat_id),
                )
            return candidate
        except sqlite3.IntegrityError:
            continue

    raise RuntimeError("Unable to generate unique anonymous token")


def get_recent_meme_video_ids(chat_id: int, since_ts: int) -> set[str]:
    with _connect(row_factory=True) as conn:
        rows = conn.execute(
            """
            SELECT video_id
            FROM meme_history
            WHERE chat_id = ?
              AND sent_at >= ?
            """,
            (chat_id, since_ts),
        ).fetchall()

    return {str(row["video_id"]) for row in rows}


def add_meme_history(chat_id: int, video_id: str, sent_at: int | None = None) -> None:
    if not video_id:
        return

    timestamp = int(sent_at if sent_at is not None else time())
    with _connect() as conn:
        conn.execute(
            """
            INSERT INTO meme_history (chat_id, video_id, sent_at)
            VALUES (?, ?, ?)
            """,
            (chat_id, video_id, timestamp),
        )

        # Keep table compact: remove entries older than 2 days.
        conn.execute(
            """
            DELETE FROM meme_history
            WHERE sent_at < ?
            """,
            (timestamp - 172800,),
        )


def _connect(row_factory: bool = False) -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path)
    if row_factory:
        conn.row_factory = sqlite3.Row
    return conn


def _row_to_settings(row: sqlite3.Row) -> GroupSettings:
    return GroupSettings(
        chat_id=int(row["chat_id"]),
        title=row["title"] or "",
        bot_enabled=bool(row["bot_enabled"]),
        moderator_trigger_enabled=bool(row["moderator_trigger_enabled"]),
        anonymous_enabled=bool(row["anonymous_enabled"]),
        anonymous_token=row["anonymous_token"],
    )
