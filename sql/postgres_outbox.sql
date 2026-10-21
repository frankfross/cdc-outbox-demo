-- Postgres equivalent of the transactional outbox used in this demo.
-- REFERENCE ONLY — the demo itself runs on SQLite so it needs no server.
--
-- The pattern is the same as the SQLite triggers: every change to the source
-- table writes an event row in the SAME transaction, so the event log can
-- never drift from the table it describes.

CREATE TABLE IF NOT EXISTS outbox (
    seq           BIGSERIAL PRIMARY KEY,
    aggregate     TEXT NOT NULL,
    aggregate_id  TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    payload       JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at  TIMESTAMPTZ,
    retry_count   INT NOT NULL DEFAULT 0,
    last_error    TEXT
);
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (seq) WHERE processed_at IS NULL;

CREATE TABLE IF NOT EXISTS outbox_dlq (LIKE outbox INCLUDING ALL);

CREATE OR REPLACE FUNCTION write_outbox_event() RETURNS trigger AS $$
DECLARE
    v_event TEXT;
    v_payload JSONB;
BEGIN
    IF TG_OP = 'INSERT' THEN
        v_event := 'order.created'; v_payload := row_to_json(NEW)::jsonb;
    ELSIF TG_OP = 'UPDATE' THEN
        v_event := 'order.updated'; v_payload := row_to_json(NEW)::jsonb;
    ELSIF TG_OP = 'DELETE' THEN
        v_event := 'order.deleted'; v_payload := row_to_json(OLD)::jsonb;
    END IF;
    INSERT INTO outbox (aggregate, aggregate_id, event_type, payload)
    VALUES ('order', COALESCE(NEW.id, OLD.id)::text, v_event, v_payload);
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_orders_outbox ON orders;
CREATE TRIGGER trg_orders_outbox
AFTER INSERT OR UPDATE OR DELETE ON orders
FOR EACH ROW EXECUTE FUNCTION write_outbox_event();
