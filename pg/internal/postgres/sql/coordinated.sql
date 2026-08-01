-- Coordinated unique keys, v2: reserve-before-commit on stock Postgres.
--
-- No node holds a physical UNIQUE index for a coordinated key. A
-- receiver-side index would fail the apply transaction before
-- arbitration could run, so enforcement is gate-only: uniqueness is
-- guaranteed by reserving the value in the cluster's registry BEFORE
-- the writing transaction commits.
--
-- Stock Postgres offers exactly one pre-commit veto point reachable
-- from a sidecar with no server extension: a DEFERRABLE INITIALLY
-- DEFERRED constraint trigger, whose ERROR aborts the commit. So:
--
--   1. Per-table BEFORE row triggers accumulate this transaction's net
--      key values into a scratch table. They do NOT talk to the network
--      -- a per-row round trip would be ruinous, and a row's value can
--      still change later in the same transaction.
--   2. One deferred constraint trigger fires at commit and sends ONE
--      batched reservation over dblink to the sidecar. A denial raises
--      23505 (unique_violation) and the commit aborts, which is exactly
--      what a UNIQUE index would have done.
--
-- The accumulator holds the NET effect: a value inserted then deleted
-- in one transaction is never reserved, and a value moved between rows
-- reserves once with its prior owner recorded, so the registry
-- transfers it rather than reporting a conflict.
--
-- Key and PK values travel as TEXT and the sidecar encodes them into
-- canonical key bytes. Canonical byte equality IS row identity, so that
-- encoding lives in exactly one implementation (the engine's Go
-- encoder, shared with capture) rather than being restated here where
-- it could drift.
--
-- Every trigger here is ENABLE ORIGIN (the default), so the apply
-- session (session_replication_role = replica) bypasses them entirely:
-- replicated rows were already reserved by their originating node.

CREATE EXTENSION IF NOT EXISTS dblink;

-- Accumulator for in-flight transactions.
--
-- UNLOGGED is load-bearing twice over: unlogged relations emit no WAL
-- for their contents, so logical decoding never sees this table and
-- capture cannot mistake scratch for data; and its contents are
-- expendable across a crash, which is correct, since an interrupted
-- transaction reserved nothing.
--
-- It is a permanent shared table rather than a TEMP one so the triggers
-- work in every session with no per-session setup step. Isolation is
-- MVCC: rows are written and consumed inside one transaction and never
-- committed, so no session observes another's.
CREATE UNLOGGED TABLE IF NOT EXISTS public.syzy_coord_pending (
    txid      bigint NOT NULL DEFAULT txid_current(),
    table_id  text   NOT NULL,
    key_id    text   NOT NULL,
    vals      text[],          -- NULL: the row vacated this key
    owner     text[] NOT NULL,
    prev      text[],
    PRIMARY KEY (txid, table_id, key_id, owner)
);

-- syzy_pk_columns returns a relation's primary-key column names in key
-- order, read from pg_catalog rather than guessed.
CREATE OR REPLACE FUNCTION public.syzy_pk_columns(rel oid)
RETURNS text[]
LANGUAGE sql STABLE AS $$
    SELECT array_agg(a.attname ORDER BY k.ord)
    FROM pg_index i
    CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
    WHERE i.indrelid = rel AND i.indisprimary
$$;

-- syzy_row_values projects the named columns out of a row's jsonb form,
-- preserving SQL NULL as a NULL array element. jsonb is the one
-- representation that lets PL/pgSQL address columns by name on a
-- rowtype it does not know at compile time.
CREATE OR REPLACE FUNCTION public.syzy_row_values(row_json jsonb, cols text[])
RETURNS text[]
LANGUAGE sql IMMUTABLE AS $$
    SELECT array_agg(
        CASE WHEN jsonb_typeof(row_json -> c) IS DISTINCT FROM 'null'
             THEN row_json ->> c END
        ORDER BY ord)
    FROM unnest(cols) WITH ORDINALITY AS u(c, ord)
$$;

-- syzy_coord_mark flags this transaction as holding unreserved keys.
--
-- A constraint trigger must be AFTER ... FOR EACH ROW, so a million-row
-- insert fires it a million times where exactly one reservation is
-- wanted. This flag is the cheap guard: a transaction-local GUC costs a
-- hash probe, where re-querying the accumulator on every firing would
-- cost an index scan per row.
--
-- The reservation clears the flag rather than the flag being set once,
-- so a transaction that runs SET CONSTRAINTS ALL IMMEDIATE and then
-- writes more keys reserves the later ones too instead of committing
-- them unreserved.
CREATE OR REPLACE FUNCTION public.syzy_coord_mark() RETURNS void
LANGUAGE sql AS $$
    SELECT set_config('syzy.coord_dirty', txid_current()::text, true)
$$;

-- syzy_coord_accum records one row's net coordinated-key effect.
--
-- The GUC pins are load-bearing, not hygiene: the sidecar re-encodes
-- these text values into canonical key bytes, so a writer session with
-- a different DateStyle or TimeZone would otherwise produce different
-- bytes for the same logical value and break row identity across nodes.
--
-- TG_ARGV[0] = table_id hex, [1] = key_id hex, [2..] = the key's column
-- names in declared order. PK column names come from syzy_pk_columns.
CREATE OR REPLACE FUNCTION public.syzy_coord_accum() RETURNS trigger
LANGUAGE plpgsql
SET DateStyle = 'ISO, MDY'
SET TimeZone = 'UTC'
SET extra_float_digits = 1
SET bytea_output = 'hex'
AS $$
DECLARE
    v_table  text   := TG_ARGV[0];
    v_key    text   := TG_ARGV[1];
    v_cols   text[] := TG_ARGV[2:];
    v_pkcols text[] := public.syzy_pk_columns(TG_RELID);
    v_owner  text[];
    v_oldpk  text[];
    v_value  text[];
    v_old    text[];
    v_prev   text[];
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_oldpk := public.syzy_row_values(to_jsonb(OLD), v_pkcols);
        -- A vacancy is recorded rather than skipped so that a value
        -- re-inserted on another row in the same transaction resolves
        -- against a row that is going away in the same commit.
        INSERT INTO public.syzy_coord_pending (table_id, key_id, vals, owner, prev)
        VALUES (v_table, v_key, NULL, v_oldpk, NULL)
        ON CONFLICT (txid, table_id, key_id, owner) DO UPDATE SET vals = NULL;
        PERFORM public.syzy_coord_mark();
        RETURN OLD;
    END IF;

    v_owner := public.syzy_row_values(to_jsonb(NEW), v_pkcols);
    v_value := public.syzy_row_values(to_jsonb(NEW), v_cols);
    IF array_position(v_value, NULL) IS NOT NULL THEN
        -- A coordinated key is NOT NULL by construction; a NULL member
        -- means this row does not participate (partial-key predicate).
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' THEN
        v_old   := public.syzy_row_values(to_jsonb(OLD), v_cols);
        v_oldpk := public.syzy_row_values(to_jsonb(OLD), v_pkcols);
        IF v_old IS NOT DISTINCT FROM v_value AND v_oldpk IS NOT DISTINCT FROM v_owner THEN
            -- Neither the key value nor the owning row changed: this row
            -- already holds the value, nothing to reserve.
            RETURN NEW;
        END IF;
        IF v_old IS NOT DISTINCT FROM v_value THEN
            -- Same value, new owning row (a PK change): a transfer, not
            -- a fresh claim.
            v_prev := v_oldpk;
        END IF;
    END IF;

    INSERT INTO public.syzy_coord_pending (table_id, key_id, vals, owner, prev)
    VALUES (v_table, v_key, v_value, v_owner, v_prev)
    ON CONFLICT (txid, table_id, key_id, owner)
    DO UPDATE SET vals = EXCLUDED.vals,
                  prev = COALESCE(public.syzy_coord_pending.prev, EXCLUDED.prev);
    PERFORM public.syzy_coord_mark();
    RETURN NEW;
END $$;

-- syzy_coord_reserve is the deferred constraint trigger body: one
-- batched reservation for the whole transaction, at commit.
CREATE OR REPLACE FUNCTION public.syzy_coord_reserve() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    v_entries jsonb;
    v_reply   text;
BEGIN
    IF current_setting('syzy.coord_dirty', true) IS DISTINCT FROM txid_current()::text THEN
        RETURN NULL;
    END IF;
    PERFORM set_config('syzy.coord_dirty', '', true);

    -- Consume: the rows must go whether or not they are reserved, so a
    -- vacancy left by a delete does not outlive its transaction.
    -- Vacancies are not reserved -- a released value re-enters the free
    -- pool through the registry's own view of replicated rows, never by
    -- a client asserting it.
    WITH consumed AS (
        DELETE FROM public.syzy_coord_pending
        WHERE txid = txid_current()
        RETURNING table_id, key_id, vals, owner, prev
    )
    SELECT jsonb_agg(jsonb_strip_nulls(jsonb_build_object(
               't', table_id, 'k', key_id, 'v', to_jsonb(vals),
               'o', to_jsonb(owner), 'p', to_jsonb(prev))))
    INTO v_entries
    FROM consumed
    WHERE vals IS NOT NULL;

    IF v_entries IS NULL THEN
        RETURN NULL;
    END IF;

    -- dblink(), not dblink_exec(): the endpoint's ErrorResponse then
    -- propagates as a raised error and aborts this commit.
    SELECT r INTO v_reply FROM dblink(
        current_setting('syzy.reserve_conninfo'),
        'RESERVE ' || encode(convert_to(jsonb_build_object('e', v_entries)::text, 'UTF8'), 'hex')
    ) AS t(r text);

    IF v_reply IS DISTINCT FROM 'ok' THEN
        RAISE EXCEPTION 'syzy: coordinated-key reservation returned %', coalesce(v_reply, '<null>')
            USING ERRCODE = '23505';
    END IF;
    RETURN NULL;
END $$;

-- syzy_coord_install wires one coordinated key on one table: an
-- accumulating BEFORE row trigger for that key, plus the table's single
-- deferred reservation trigger (shared by all of its keys).
--
-- When a transaction touches several coordinated tables, each table's
-- constraint trigger fires, the first reserves the whole batch, and the
-- rest find the flag cleared -- one round trip per transaction, not one
-- per table.
CREATE OR REPLACE FUNCTION public.syzy_coord_install(
    rel regclass, table_id text, key_id text, cols text[]
) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_args text;
BEGIN
    SELECT string_agg(quote_literal(a), ', ' ORDER BY ord)
    INTO v_args
    FROM unnest(array[table_id, key_id] || cols) WITH ORDINALITY AS u(a, ord);

    EXECUTE format(
        'CREATE OR REPLACE TRIGGER %I BEFORE INSERT OR UPDATE OR DELETE ON %s '
        'FOR EACH ROW EXECUTE FUNCTION public.syzy_coord_accum(%s)',
        'syzy_coord_accum_' || key_id, rel::text, v_args);

    -- CREATE OR REPLACE is rejected for constraint triggers, so this one
    -- is dropped and recreated. Both statements run inside the installing
    -- transaction, so no concurrent writer ever sees the table ungated.
    EXECUTE format('DROP TRIGGER IF EXISTS syzy_coord_reserve ON %s', rel::text);
    EXECUTE format(
        'CREATE CONSTRAINT TRIGGER syzy_coord_reserve '
        'AFTER INSERT OR UPDATE OR DELETE ON %s '
        'DEFERRABLE INITIALLY DEFERRED FOR EACH ROW '
        'EXECUTE FUNCTION public.syzy_coord_reserve()', rel::text);
END $$;

-- syzy_coord_uninstall drops one key's accumulating trigger, and the
-- table's reservation trigger once no coordinated key remains.
CREATE OR REPLACE FUNCTION public.syzy_coord_uninstall(rel regclass, key_id text)
RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s',
                   'syzy_coord_accum_' || key_id, rel::text);
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid = rel AND NOT tgisinternal
          AND tgname LIKE 'syzy\_coord\_accum\_%'
    ) THEN
        EXECUTE format('DROP TRIGGER IF EXISTS syzy_coord_reserve ON %s', rel::text);
    END IF;
END $$;
