"""The outbox poller (message relay).

The poller fetches unprocessed outbox rows in `seq` order and hands each one to
a handler. The handler's downstream write and the `processed_at` mark happen
in a single transaction, so a crash between applying and marking is recovered
by simply re-running: the event is still unprocessed and gets applied again.
Handlers must therefore be idempotent — the included `mirror_handler` is.

Failure policy: an exception increments `retry_count`. After `max_retries`
the event moves to `outbox_dlq` with its final error so one poison event can
never block the queue behind it.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Callable

EventHandler = Callable[[sqlite3.Connection, sqlite3.Row], None]

FETCH_SQL = """
SELECT seq, aggregate, aggregate_id, event_type, payload,
       created_at, retry_count, last_error
FROM outbox
WHERE processed_at IS NULL
ORDER BY seq
LIMIT ?
"""


class OutboxPoller:
    def __init__(self, conn: sqlite3.Connection, handler: EventHandler,
                 batch_size: int = 100, max_retries: int = 3):
        self.conn = conn
        self.handler = handler
        self.batch_size = batch_size
        self.max_retries = max_retries

    def pending_count(self) -> int:
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM outbox WHERE processed_at IS NULL"
        ).fetchone()
        return row["n"]

    def dlq_count(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS n FROM outbox_dlq").fetchone()
        return row["n"]

    def run_once(self) -> dict:
        """Process up to one batch. Returns {applied, failed, dead_lettered}."""
        stats = {"applied": 0, "failed": 0, "dead_lettered": 0}
        events = self.conn.execute(FETCH_SQL, (self.batch_size,)).fetchall()
        for event in events:
            outcome = self._process(event)
            stats[outcome] += 1
        return stats

    def run_until_empty(self, max_passes: int = 1000) -> dict:
        """Keep polling until no pending events remain (or passes are exhausted)."""
        total = {"applied": 0, "failed": 0, "dead_lettered": 0}
        for _ in range(max_passes):
            if self.pending_count() == 0:
                break
            stats = self.run_once()
            for k in total:
                total[k] += stats[k]
        return total

    def _process(self, event: sqlite3.Row) -> str:
        # Each event gets its own transaction: the handler's downstream write
        # and the processed-mark commit together (or roll back together).
        try:
            with self.conn:  # commits on success, rolls back on exception
                self.handler(self.conn, event)
                self.conn.execute(
                    "UPDATE outbox SET processed_at = datetime('now') WHERE seq = ?",
                    (event["seq"],),
                )
        except Exception as exc:  # noqa: BLE001 — we record whatever failed
            retries = event["retry_count"] + 1
            if retries > self.max_retries:
                with self.conn:
                    self.conn.execute(
                        """INSERT INTO outbox_dlq
                           (seq, aggregate, aggregate_id, event_type, payload,
                            created_at, retry_count, last_error)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                        (event["seq"], event["aggregate"], event["aggregate_id"],
                         event["event_type"], event["payload"], event["created_at"],
                         retries, str(exc)),
                    )
                    self.conn.execute(
                        "UPDATE outbox SET processed_at = datetime('now'), "
                        "retry_count = ?, last_error = ? WHERE seq = ?",
                        (retries, str(exc), event["seq"]),
                    )
                return "dead_lettered"
            with self.conn:
                self.conn.execute(
                    "UPDATE outbox SET retry_count = ?, last_error = ? WHERE seq = ?",
                    (retries, str(exc), event["seq"]),
                )
            return "failed"
        return "applied"


def make_mirror_table(conn: sqlite3.Connection) -> None:
    """A downstream replica table the handler keeps in sync with `orders`."""
    conn.execute(
        """CREATE TABLE IF NOT EXISTS warehouse_orders (
               id INTEGER PRIMARY KEY, customer TEXT NOT NULL,
               amount REAL NOT NULL, status TEXT NOT NULL)"""
    )
    conn.commit()


def mirror_handler(conn: sqlite3.Connection, event: sqlite3.Row) -> None:
    """Idempotent handler: apply order events to the downstream replica."""
    payload = json.loads(event["payload"])
    if event["event_type"] in ("order.created", "order.updated"):
        conn.execute(
            """INSERT INTO warehouse_orders (id, customer, amount, status)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 customer = excluded.customer,
                 amount = excluded.amount,
                 status = excluded.status""",
            (payload["id"], payload["customer"], payload["amount"], payload["status"]),
        )
    elif event["event_type"] == "order.deleted":
        conn.execute("DELETE FROM warehouse_orders WHERE id = ?", (payload["id"],))
    else:
        raise ValueError(f"unknown event_type: {event['event_type']!r}")
