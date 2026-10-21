"""Transactional outbox pattern, end to end, on SQLite."""

from outbox.schema import connect, init_db
from outbox.poller import OutboxPoller, make_mirror_table, mirror_handler


def main() -> None:
    conn = connect()
    init_db(conn)
    make_mirror_table(conn)

    print("== 1. Write to the source table (each change fires a trigger) ==")
    conn.executemany(
        "INSERT INTO orders (customer, amount, status) VALUES (?, ?, ?)",
        [("Acme", 120.50, "new"), ("Globex", 44.00, "new"),
         ("Initech", 899.99, "new"), ("Umbrella", 12.75, "new")],
    )
    conn.execute("UPDATE orders SET status = 'paid' WHERE id = 2")
    conn.execute("UPDATE orders SET amount = 950.00 WHERE id = 3")
    conn.execute("DELETE FROM orders WHERE id = 4")
    conn.commit()

    print("\n== 2. What the triggers wrote to the outbox ==")
    for row in conn.execute(
        "SELECT seq, event_type, aggregate_id, retry_count, processed_at IS NULL AS pending "
        "FROM outbox ORDER BY seq"
    ):
        print(f"  seq={row['seq']:>2}  {row['event_type']:<14} order#{row['aggregate_id']}  "
              f"retries={row['retry_count']} pending={bool(row['pending'])}")

    print("\n== 3. Poller drains the outbox into the downstream replica ==")
    poller = OutboxPoller(conn, mirror_handler)
    stats = poller.run_until_empty()
    print(f"  applied={stats['applied']} failed={stats['failed']} "
          f"dead_lettered={stats['dead_lettered']}")

    print("\n== 4. Downstream replica now mirrors the source table ==")
    for row in conn.execute("SELECT id, customer, amount, status FROM warehouse_orders ORDER BY id"):
        print(f"  id={row['id']} {row['customer']:<9} ${row['amount']:<8} {row['status']}")

    print("\n== 5. Source and replica agree ==")
    src = conn.execute("SELECT id, customer, amount, status FROM orders ORDER BY id").fetchall()
    dst = conn.execute("SELECT id, customer, amount, status FROM warehouse_orders ORDER BY id").fetchall()
    print(f"  source rows={len(src)} replica rows={len(dst)} in_sync={src == dst}")


if __name__ == "__main__":
    main()
