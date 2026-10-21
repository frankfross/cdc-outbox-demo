"""Tests for the transactional outbox demo."""

import json

import pytest

from outbox.schema import connect, init_db
from outbox.poller import OutboxPoller, make_mirror_table, mirror_handler


@pytest.fixture
def conn():
    c = connect()
    init_db(c)
    make_mirror_table(c)
    yield c
    c.close()


def outbox_rows(c):
    return c.execute("SELECT * FROM outbox ORDER BY seq").fetchall()


def test_insert_trigger_writes_event(conn):
    conn.execute("INSERT INTO orders (customer, amount, status) VALUES ('Acme', 10.0, 'new')")
    conn.commit()
    rows = outbox_rows(conn)
    assert len(rows) == 1
    ev = rows[0]
    assert ev["event_type"] == "order.created"
    assert ev["aggregate"] == "order"
    assert ev["aggregate_id"] == "1"
    assert json.loads(ev["payload"])["customer"] == "Acme"
    assert ev["processed_at"] is None


def test_update_trigger_writes_event(conn):
    conn.execute("INSERT INTO orders (customer, amount) VALUES ('Acme', 10.0)")
    conn.execute("UPDATE orders SET status = 'paid' WHERE id = 1")
    conn.commit()
    rows = outbox_rows(conn)
    assert [r["event_type"] for r in rows] == ["order.created", "order.updated"]
    assert json.loads(rows[1]["payload"])["status"] == "paid"


def test_delete_trigger_writes_event(conn):
    conn.execute("INSERT INTO orders (customer, amount) VALUES ('Acme', 10.0)")
    conn.execute("DELETE FROM orders WHERE id = 1")
    conn.commit()
    rows = outbox_rows(conn)
    assert [r["event_type"] for r in rows] == ["order.created", "order.deleted"]
    assert json.loads(rows[1]["payload"])["id"] == 1


def test_event_written_in_same_transaction_as_change(conn):
    # A rolled-back change must not leave an outbox row behind.
    conn.execute("INSERT INTO orders (customer, amount) VALUES ('Acme', 10.0)")
    conn.rollback()
    assert outbox_rows(conn) == []


def test_poller_applies_in_order_and_marks_processed(conn):
    conn.execute("INSERT INTO orders (customer, amount) VALUES ('Acme', 10.0)")
    conn.execute("UPDATE orders SET amount = 20.0 WHERE id = 1")
    conn.commit()
    poller = OutboxPoller(conn, mirror_handler)
    stats = poller.run_until_empty()
    assert stats == {"applied": 2, "failed": 0, "dead_lettered": 0}
    assert poller.pending_count() == 0
    row = conn.execute("SELECT * FROM warehouse_orders WHERE id = 1").fetchone()
    assert row["amount"] == 20.0


def test_poller_applies_deletes(conn):
    conn.execute("INSERT INTO orders (customer, amount) VALUES ('Acme', 10.0)")
    conn.commit()
    OutboxPoller(conn, mirror_handler).run_until_empty()
    conn.execute("DELETE FROM orders WHERE id = 1")
    conn.commit()
    stats = OutboxPoller(conn, mirror_handler).run_until_empty()
    assert stats["applied"] == 1
    assert conn.execute("SELECT COUNT(*) AS n FROM warehouse_orders").fetchone()["n"] == 0


def test_rerun_is_noop(conn):
    conn.execute("INSERT INTO orders (customer, amount) VALUES ('Acme', 10.0)")
    conn.commit()
    poller = OutboxPoller(conn, mirror_handler)
    poller.run_until_empty()
    assert poller.run_until_empty() == {"applied": 0, "failed": 0, "dead_lettered": 0}
    assert conn.execute("SELECT COUNT(*) AS n FROM warehouse_orders").fetchone()["n"] == 1


def test_failing_handler_retries_then_dead_letters(conn):
    def always_fails(c, event):
        raise RuntimeError("downstream is down")

    conn.execute("INSERT INTO orders (customer, amount) VALUES ('Acme', 10.0)")
    conn.commit()
    poller = OutboxPoller(conn, always_fails, max_retries=2)
    for _ in range(2):
        assert poller.run_once() == {"applied": 0, "failed": 1, "dead_lettered": 0}
        assert poller.pending_count() == 1
    # third failure exceeds max_retries -> DLQ
    assert poller.run_once() == {"applied": 0, "failed": 0, "dead_lettered": 1}
    assert poller.pending_count() == 0
    assert poller.dlq_count() == 1
    dlq = conn.execute("SELECT * FROM outbox_dlq").fetchone()
    assert dlq["seq"] == 1
    assert "downstream is down" in dlq["last_error"]
    assert dlq["retry_count"] == 3


def test_failed_event_does_not_block_later_events(conn):
    calls = []

    def flaky(c, event):
        calls.append(event["seq"])
        if event["seq"] == 1:
            raise RuntimeError("poison")

    conn.execute("INSERT INTO orders (customer, amount) VALUES ('Acme', 10.0)")
    conn.execute("INSERT INTO orders (customer, amount) VALUES ('Globex', 5.0)")
    conn.commit()
    poller = OutboxPoller(conn, flaky, max_retries=0)  # 0 retries -> DLQ on first failure
    stats = poller.run_until_empty()
    assert stats == {"applied": 1, "failed": 0, "dead_lettered": 1}
    assert calls == [1, 2]


def test_handler_errors_on_unknown_event_type(conn):
    conn.execute(
        "INSERT INTO outbox (aggregate, aggregate_id, event_type, payload) "
        "VALUES ('order', '9', 'order.mystery', '{}')"
    )
    conn.commit()
    poller = OutboxPoller(conn, mirror_handler, max_retries=0)
    assert poller.run_once() == {"applied": 0, "failed": 0, "dead_lettered": 1}
