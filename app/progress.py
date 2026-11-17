"""Append-only disposal-progress feed and cursor-paged catch-up reads.

Two guarantees live here:

1. **Atomic state + node.** ``add_node`` runs inside the caller's open write
   transaction (the same one that moves the alarm/notification state). A node
   therefore commits and becomes readable exactly when its disposal state does,
   and never otherwise. Nodes are insert-only; nothing ever updates or deletes
   a committed row, so a node already read stays byte-for-byte the same after
   later confirmations or receipt discoveries. Its ``status_snapshot`` is the
   alarm status frozen at that moment.

2. **Stable catch-up window.** A catch-up session pins the high-water mark
   (``end_seq``) at creation time. Every page and every retry of that session
   reads the same fixed range; concurrent deliveries, confirmations and
   restarts can append new nodes but cannot shift, drop or rewrite the pages
   already in the window. Once caught up, the client continues tailing from the
   last cursor without a session, so every later committed node is eventually
   delivered exactly once per cursor position.

Dedupe is enforced twice: by the service logic (idempotent retries and repeat
confirmations usually never reach the insert) and by the database's
``UNIQUE`` constraints, which are the final word even under concurrency.
"""

from __future__ import annotations

import json
import secrets
import time
from typing import Any

from .db import Database, iso_from_real, parse_ts, utc_now

# Human-readable labels for Swagger/operator clients.
NODE_TYPE_TEXT = {
    "alarm_submitted": "告警提交",
    "primary_accepted": "主通知被网关受理",
    "primary_rejected": "主通知被网关拒收",
    "backup_generated": "备班通知生成",
    "confirmed": "人工确认",
}

# How long an idle catch-up session remains reusable. Sessions are tiny and
# cleaned up lazily on creation; the window itself is immutable even after the
# session expires, the client simply tails live nodes afterwards.
CATCHUP_TTL_SECONDS = 24 * 3600


class CatchupNotFoundError(Exception):
    pass


def _cabinet_no(alarm_row: dict[str, Any] | None) -> str | None:
    """Best-effort cabinet number from the alarm payload.

    The cabinet is optional information ("柜号（若有）"). The canonical location
    is ``metadata.cabinet``; a few common aliases are accepted as well. A
    non-string or missing value yields ``None`` rather than failing the write.
    """
    if alarm_row is None:
        return None
    try:
        payload = json.loads(alarm_row["payload_json"])
    except (KeyError, TypeError, ValueError):
        return None
    metadata = payload.get("metadata") if isinstance(payload, dict) else None
    if not isinstance(metadata, dict):
        return None
    for key in ("cabinet", "cabinet_no", "cabinet_id"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, int):
            return str(value)
    return None


def node_id_for(alarm_no: str, node_type: str, role: str | None = None) -> str:
    """Deterministic idempotency key for a node.

    Re-deriving the same key on every attempt makes the insert safely
    repeatable: the unique index rejects the duplicate instead of appending a
    second node.
    """
    if role:
        return f"{alarm_no}:{node_type}:{role}"
    return f"{alarm_no}:{node_type}"


class ProgressService:
    def __init__(self, db: Database):
        self.db = db

    # ------------------------------------------------------------- writing nodes

    def add_node(
        self,
        conn,
        *,
        alarm_no: str,
        node_type: str,
        status: str,
        status_snapshot: dict[str, Any],
        detail: str | None = None,
        notification_no: str | None = None,
        notification_role: str | None = None,
        occurred_real: float | None = None,
    ) -> bool:
        """Insert a progress node inside the caller's transaction.

        Returns True when a new row was inserted, False when the node already
        existed (idempotent retry / duplicate). Never raises on a dedupe hit —
        the surrounding state transition still commits normally.
        """
        nid = node_id_for(alarm_no, node_type, notification_role)
        existing = conn.execute(
            "SELECT 1 FROM progress_nodes WHERE node_id = ?", (nid,)
        ).fetchone()
        if existing is not None:
            return False

        now_real = occurred_real if occurred_real is not None else parse_ts(utc_now())
        alarm = conn.execute(
            "SELECT payload_json FROM alarms WHERE alarm_no = ?", (alarm_no,)
        ).fetchone()
        cabinet = _cabinet_no(dict(alarm) if alarm is not None else None)
        cursor_obj = conn.execute(
            """
            INSERT OR IGNORE INTO progress_nodes (
                node_id, alarm_no, cabinet_no, node_type, occurred_at, occurred_real,
                alarm_status, status_snapshot, notification_no, notification_role, detail
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                nid,
                alarm_no,
                cabinet,
                node_type,
                iso_from_real(now_real),
                now_real,
                status,
                json.dumps(status_snapshot, ensure_ascii=False, sort_keys=True),
                notification_no,
                notification_role,
                detail,
            ),
        )
        return cursor_obj.rowcount > 0

    def max_seq(self) -> int:
        row = self.db.query_one("SELECT COALESCE(MAX(seq), 0) AS m FROM progress_nodes")
        return int(row["m"]) if row else 0

    # ------------------------------------------------------------- catch-up API

    def create_catchup(self, start_cursor: int | None = None) -> dict[str, Any]:
        """Pin a catch-up window. ``start_cursor`` is the last cursor the client
        already consumed (0/None means the whole history); ``end_seq`` fixes the
        inclusive high-water mark for this catch-up session."""
        now_real = time.time()
        with self.db.transaction() as conn:
            conn.execute(
                "DELETE FROM catchup_sessions WHERE expires_real < ?", (now_real,)
            )
            current_max = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS m FROM progress_nodes"
            ).fetchone()["m"]
            start = max(0, int(start_cursor or 0))
            session_id = secrets.token_urlsafe(24)
            created_at = utc_now()
            conn.execute(
                """
                INSERT INTO catchup_sessions
                    (session_id, start_cursor, end_seq, created_at, expires_real)
                VALUES (?, ?, ?, ?, ?)
                """,
                (session_id, start, current_max, created_at, now_real + CATCHUP_TTL_SECONDS),
            )
            return {
                "session_id": session_id,
                "start_cursor": start,
                "end_seq": int(current_max),
                "created_at": created_at,
                "expires_in_seconds": CATCHUP_TTL_SECONDS,
            }

    def get_session(self, session_id: str) -> dict[str, Any]:
        row = self.db.query_one(
            "SELECT * FROM catchup_sessions WHERE session_id = ?", (session_id,)
        )
        if row is None:
            raise CatchupNotFoundError(session_id)
        return dict(row)

    def read_page(
        self,
        *,
        limit: int,
        cursor: int = 0,
        session_id: str | None = None,
        alarm_no: str | None = None,
    ) -> dict[str, Any]:
        """Return one stable, cursor-paged slice of the feed.

        With a session, reads are bounded by the pinned ``end_seq``: retries
        with the same cursor always return the same nodes even while new nodes
        are being committed. Without a session (live tailing), the current end
        of the feed is used, so polling from the last cursor eventually yields
        every committed node.
        """
        cursor = max(0, int(cursor))
        if session_id is not None:
            session = self.get_session(session_id)
            start = max(cursor, int(session["start_cursor"]))
            end_seq = int(session["end_seq"])
        else:
            session = None
            start = cursor
            end_seq = self.max_seq()

        params: list[Any] = [start]
        where = ["seq > ?"]
        if session_id is not None:
            where.append("seq <= ?")
            params.append(end_seq)
        if alarm_no:
            where.append("alarm_no = ?")
            params.append(alarm_no)
        params.append(limit + 1)

        rows = self.db.query(
            f"SELECT * FROM progress_nodes WHERE {' AND '.join(where)} ORDER BY seq ASC LIMIT ?",
            tuple(params),
        )
        has_more = len(rows) > limit
        page = [self._node_view(dict(r)) for r in rows[:limit]]
        next_cursor = page[-1]["seq"] if page else start

        return {
            "items": page,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "window_end_seq": end_seq,
            "session_id": session_id,
            "caught_up": (not has_more) and next_cursor >= end_seq,
        }

    # ------------------------------------------------------------------ views

    @staticmethod
    def _node_view(row: dict[str, Any]) -> dict[str, Any]:
        try:
            snapshot = json.loads(row["status_snapshot"])
        except (TypeError, ValueError):
            snapshot = {}
        return {
            "seq": row["seq"],
            "node_id": row["node_id"],
            "alarm_no": row["alarm_no"],
            "cabinet_no": row.get("cabinet_no"),
            "node_type": row["node_type"],
            "node_type_text": NODE_TYPE_TEXT.get(row["node_type"], row["node_type"]),
            "occurred_at": row["occurred_at"],
            "alarm_status": row["alarm_status"],
            "alarm_status_text": snapshot.get("status_text", row["alarm_status"]),
            "status_snapshot": snapshot,
            "notification_no": row.get("notification_no"),
            "notification_role": row.get("notification_role"),
            "detail": row.get("detail"),
        }
