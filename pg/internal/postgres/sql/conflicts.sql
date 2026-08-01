-- syzy_conflicts: the audit surface for every arbitration that discarded a
-- committed write (docs/postgres.md §9). Node-local — it carries no catalog
-- entry, so capture skips its rows exactly as it skips the DDL spool's, and it
-- is never replicated. Query it with psql; nothing in the engine reads it.
--
-- Both sides of an override are recorded: loser_side = 'local' when an inbound
-- write overrode values this node had committed, 'inbound' when this node's
-- state overrode a peer's write. Retention is bounded (the writer prunes to a
-- fixed row count), so an operator can leave it alone forever.
CREATE TABLE IF NOT EXISTS public.syzy_conflicts (
    seq            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    at             timestamptz NOT NULL DEFAULT now(),
    tbl            text        NOT NULL,
    pk             text        NOT NULL,
    -- 'concurrent': same generation, different origins — neither write is
    -- provably later, so this is a genuine clobber.
    -- 'superseded': the loser belongs to an older generation (the row was
    -- deleted and recreated); its values could not have survived.
    kind           text        NOT NULL,
    loser_side     text        NOT NULL,
    -- The losing write's shape; a losing delete is the one loss with no values.
    op             text        NOT NULL,
    cols           text[]      NOT NULL,
    lost_values    jsonb       NOT NULL,
    winner_origin  bigint      NOT NULL,
    winner_wall    bigint      NOT NULL,
    winner_logical bigint      NOT NULL,
    winner_cl      bigint      NOT NULL,
    loser_origin   bigint      NOT NULL,
    loser_wall     bigint      NOT NULL,
    loser_logical  bigint      NOT NULL,
    loser_cl       bigint      NOT NULL
);
