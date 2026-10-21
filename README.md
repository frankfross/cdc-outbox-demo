# cdc-outbox-demo

A minimal, working demo of the **transactional outbox pattern** for reliable
change-data capture: every change to a source table publishes an event in the
*same database transaction*, so the event log can never drift from the data it
describes. A poller (message relay) then drains the log into a downstream
system, with retries and a dead-letter queue for poison events.

This is the pattern behind "exactly-once-ish" CDC without a CDC platform: no
Kafka, no Debezium, no polling the source table — just triggers and one
`seq`-ordered table.

## How it works

1. **Triggers capture changes.** `AFTER INSERT/UPDATE/DELETE` triggers on
   `orders` write one row per change into the `outbox` table — in the same
   transaction as the change itself. If the change rolls back, the event
   rolls back too (tested).
2. **The poller relays events.** `OutboxPoller` fetches unprocessed rows in
   `seq` order and hands each to a handler. The handler's downstream write
   and the `processed_at` mark commit in **one transaction**: a crash between
   applying and marking is recovered by re-running, so handlers must be
   idempotent (the included `mirror_handler` is — it upserts).
3. **Failures don't jam the queue.** A handler exception increments
   `retry_count`; after `max_retries` the event moves to `outbox_dlq` with its
   final error, and the poller moves on to the next event.

## Quickstart

```bash
pip install -r requirements.txt   # pytest, for tests
python3 -m outbox.demo
```

Example output:

```
== 1. Write to the source table (each change fires a trigger) ==

== 2. What the triggers wrote to the outbox ==
  seq= 1  order.created  order#1  retries=0 pending=True
  seq= 2  order.created  order#2  retries=0 pending=True
  seq= 3  order.created  order#3  retries=0 pending=True
  seq= 4  order.created  order#4  retries=0 pending=True
  seq= 5  order.updated  order#2  retries=0 pending=True
  seq= 6  order.updated  order#3  retries=0 pending=True
  seq= 7  order.deleted  order#4  retries=0 pending=True

== 3. Poller drains the outbox into the downstream replica ==
  applied=7 failed=0 dead_lettered=0

== 4. Downstream replica now mirrors the source table ==
  id=1 Acme      $120.5    new
  id=2 Globex    $44.0     paid
  id=3 Initech   $950.0    new

== 5. Source and replica agree ==
  source rows=3 replica rows=3 in_sync=True
```

Run the tests:

```bash
python3 -m pytest tests/ -q
```

## Design notes

- **Why outbox instead of dual writes?** Writing to the source table and then
  to a message broker in two steps can leave them inconsistent (broker
  publish succeeds, DB commit fails, or vice versa). The outbox keeps both
  writes in one transaction and publishes later — the poller is the only
  component that can fail independently, and it retries.
- **Ordering:** events are applied in `seq` order. This is per-aggregate in
  real systems (partition by `aggregate_id`); the demo uses one global
  sequence for clarity.
- **Postgres version:** `sql/postgres_outbox.sql` is the equivalent DDL with
  a `plpgsql` trigger — reference only. The demo runs on SQLite so it needs
  no server; the trigger logic maps 1:1.
- **What this is not:** it doesn't claim CDC at scale — no partitioning, no
  exactly-once broker semantics, no schema evolution. It's the core loop,
  small enough to read in one sitting.

## Files

- `outbox/schema.py` — tables + triggers, `init_db(conn)`
- `outbox/poller.py` — `OutboxPoller`, retry/DLQ logic, `mirror_handler`
- `outbox/demo.py` — end-to-end walkthrough (`python3 -m outbox.demo`)
- `sql/postgres_outbox.sql` — Postgres equivalent DDL (reference)
- `tests/test_outbox.py` — 10 pytest tests covering triggers, ordering,
  transactional rollback, retries, DLQ, and poison-event isolation
