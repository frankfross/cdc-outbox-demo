"""Core schema for the transactional outbox demo.

Sets up:
  - `orders`          : the source "service" table whose changes we want to publish
  - `outbox`          : the event log; rows are written by triggers in the same
                        transaction as the change that produced them
  - `outbox_dlq`      : poison events that exhausted their retries
  - SQLite triggers   : AFTER INSERT/UPDATE/DELETE on orders -> INSERT into outbox

Everything runs on SQLite so the demo needs no server. The trigger logic maps
1:1 to Postgres (see sql/postgres_outbox.sql for the equivalent DDL).
"""

from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id          INTEGER PRIMARY KEY,
    customer    TEXT NOT NULL,
    amount      REAL NOT NULL,
    status      TEXT NOT NULL DEFAULT 'new',
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS outbox (
    seq           INTEGER PRIMARY KEY AUTOINCREMENT,
    aggregate     TEXT NOT NULL,
    aggregate_id  TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload       TEXT NOT NULL,              -- JSON
    created_at    TEXT NOT NULL DEFAULT (datetime('now')),
    processed_at  TEXT,                       -- NULL until the poller applies it
    retry_count   INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox (seq) WHERE processed_at IS NULL;

CREATE TABLE IF NOT EXISTS outbox_dlq (
    seq           INTEGER PRIMARY KEY,
    aggregate     TEXT NOT NULL,
    aggregate_id  TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload       TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    failed_at     TEXT NOT NULL DEFAULT (datetime('now')),
    retry_count   INTEGER NOT NULL,
    last_error    TEXT NOT NULL
);

DROP TRIGGER IF EXISTS trg_orders_insert;
CREATE TRIGGER trg_orders_insert AFTER INSERT ON orders
BEGIN
    INSERT INTO outbox (aggregate, aggregate_id, event_type, payload)
    VALUES (
        'order',
        CAST(NEW.id AS TEXT),
        'order.created',
        json_object('id', NEW.id, 'customer', NEW.customer,
                    'amount', NEW.amount, 'status', NEW.status)
    );
END;

DROP TRIGGER IF EXISTS trg_orders_update;
CREATE TRIGGER trg_orders_update AFTER UPDATE ON orders
BEGIN
    INSERT INTO outbox (aggregate, aggregate_id, event_type, payload)
    VALUES (
        'order',
        CAST(NEW.id AS TEXT),
        'order.updated',
        json_object('id', NEW.id, 'customer', NEW.customer,
                    'amount', NEW.amount, 'status', NEW.status)
    );
END;

DROP TRIGGER IF EXISTS trg_orders_delete;
CREATE TRIGGER trg_orders_delete AFTER DELETE ON orders
BEGIN
    INSERT INTO outbox (aggregate, aggregate_id, event_type, payload)
    VALUES (
        'order',
        CAST(OLD.id AS TEXT),
        'order.deleted',
        json_object('id', OLD.id, 'customer', OLD.customer,
                    'amount', OLD.amount, 'status', OLD.status)
    );
END;
"""


def connect(path: str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables + triggers. Idempotent — safe to call on an existing DB."""
    conn.executescript(SCHEMA)
    conn.commit()
