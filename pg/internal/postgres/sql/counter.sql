-- Cell clock group + counter columns (docs/postgres.md §8).
--
-- Two node-local objects, installed on every node whether or not DDL
-- replication is enabled (a pre-created bootstrap table may declare a counter
-- column):
--
--   syzy_counter — the domain that DECLARES a counter column. Postgres has no
--   per-column annotation a schema can carry, so the declaration rides the
--   column's type, exactly as SQLite's "INTEGER COUNTER" does. It is a plain
--   bigint underneath: arithmetic, indexes and I/O behave identically, and the
--   sidecar reads the domain to decide the column merges by summation.
--
--   syzy_applied — the counter apply exactly-once marker. Summation is not
--   idempotent, and the sidecar's frontier is persisted OUTSIDE the Postgres
--   transaction that lands the deltas, so a crash between the two would
--   re-deliver a counter-bearing changeset. The marker is written INSIDE the
--   apply transaction — exactly as durable as the deltas it certifies — and on
--   re-delivery the contributions are stripped and only the idempotent
--   remainder re-applies.
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public' AND t.typname = 'syzy_counter'
    ) THEN
        CREATE DOMAIN public.syzy_counter AS bigint;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.syzy_applied (
    origin bigint NOT NULL,
    seq    bigint NOT NULL,
    PRIMARY KEY (origin, seq)
);
