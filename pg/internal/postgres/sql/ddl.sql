-- DDL replication support (§6), increment A: the syzy_ddl_intent spool + the
-- event triggers that fill it. DDL leaves no decodable trace of its own (logical
-- decoding is DML-only), so the trigger persists the full structured descriptor
-- of each command INSIDE the user's transaction — co-transactional, so a
-- ROLLBACK discards it and a multi-statement migration accumulates one
-- ordinal-tagged row per command. Capture decodes these rows like any DML
-- (the publication is FOR ALL TABLES), exactly as it decodes syzy_blob_intent.
--
-- pg_event_trigger_ddl_commands() / pg_event_trigger_dropped_objects() are
-- callable ONLY inside an event trigger, so the descriptor MUST be persisted
-- here: capture cannot reconstruct it post-commit. current_query() is stored as
-- audit text only — the typed CatalogOp is later built from the catalog keyed by
-- (classid, objid, objsubid), never parsed from this SQL.

CREATE TABLE IF NOT EXISTS syzy_ddl_intent (
    seq             bigserial PRIMARY KEY,
    txid            bigint  NOT NULL,        -- groups one transaction's commands
    ordinal         int     NOT NULL,        -- per-command order within the txn
    command_tag     text    NOT NULL,        -- 'CREATE TABLE', 'ALTER TABLE', 'DROP', ...
    object_type     text,                    -- 'table','index','view','column',...
    classid         oid     NOT NULL,        -- catalog the object lives in
    objid           oid     NOT NULL,        -- object's oid
    objsubid        int     NOT NULL,        -- column attnum (0 for whole-object)
    schema_name     text,
    object_identity text,                    -- qualified identity
    is_drop         boolean NOT NULL DEFAULT false,
    audit_query     text                     -- current_query(): AUDIT TEXT ONLY
);

-- syzy_ddl_next_ordinal hands out a per-command ordinal within the current
-- transaction. The counter is a txn-local GUC, so it resets to 0 automatically
-- at the next transaction with no cleanup.
-- SECURITY DEFINER + a pinned search_path on every trigger function: the
-- functions run as the (syzy) owner, so an application migration role that
-- lacks INSERT on syzy_ddl_intent still triggers cleanly ("apps run ordinary
-- DDL"), and the fixed search_path resolves the internal objects (public) and
-- built-ins (pg_catalog) regardless of the migration session's search_path.
CREATE OR REPLACE FUNCTION syzy_ddl_next_ordinal() RETURNS int
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE n int;
BEGIN
    -- NULLIF, not just COALESCE: once a placeholder GUC has been set in a
    -- session, reverting the transaction-local value leaves current_setting
    -- returning '' rather than NULL — so every DDL transaction after the first
    -- one on a connection would fail on ''::int.
    n := COALESCE(NULLIF(current_setting('syzy.ddl_ordinal', true), ''), '0')::int;
    PERFORM set_config('syzy.ddl_ordinal', (n + 1)::text, true);  -- true = txn-local
    RETURN n;
END $$;

-- Admission (§6 G) needs the column shape the ALTER started from, and
-- ddl_command_end sees only the finished catalog. So a ddl_command_start
-- trigger — which runs before the command touches anything — records the
-- pre-command shape of every replicated table in this backend's slot of
-- syzy_ddl_prior. It is UNLOGGED and transaction-scoped in effect: a ROLLBACK
-- discards the rows, decoding never sees them, and the next command in the same
-- session replaces them. (The target relation is not known at command_start —
-- pg_event_trigger_ddl_commands() is end-only and the SQL text is never parsed
-- — hence the whole replicated schema.)
CREATE UNLOGGED TABLE IF NOT EXISTS syzy_ddl_prior (
    pid          int     NOT NULL,
    relid        oid     NOT NULL,
    attname      name    NOT NULL,
    typename     text    NOT NULL,
    is_notnull   boolean NOT NULL,
    is_generated boolean NOT NULL,
    identity     "char"  NOT NULL,
    pkpos        int     NOT NULL,
    -- The table's pre-command pg_class.relreplident, repeated on each of its
    -- rows: REPLICA IDENTITY FULL is the cell-clock-group opt-in, so the
    -- admission gate has to know which side of the flip the command started on.
    replident    "char"  NOT NULL DEFAULT 'd',
    PRIMARY KEY (pid, relid, attname)
);
-- Upgrade path for a node whose spool predates the column.
ALTER TABLE syzy_ddl_prior ADD COLUMN IF NOT EXISTS replident "char" NOT NULL DEFAULT 'd';

CREATE OR REPLACE FUNCTION syzy_ddl_snapshot() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    IF current_setting('syzy.internal', true) = 'on' THEN RETURN; END IF;
    DELETE FROM syzy_ddl_prior WHERE pid = pg_backend_pid();
    -- Rows a backend left behind by disconnecting mid-DDL: reclaimed here
    -- rather than by a background sweep.
    DELETE FROM syzy_ddl_prior p
    WHERE NOT EXISTS (SELECT 1 FROM pg_stat_activity s WHERE s.pid = p.pid);
    INSERT INTO syzy_ddl_prior
      (pid, relid, attname, typename, is_notnull, is_generated, identity, pkpos, replident)
    SELECT pg_backend_pid(), a.attrelid, a.attname,
           format_type(a.atttypid, a.atttypmod), a.attnotnull,
           a.attgenerated <> '', a.attidentity, COALESCE(k.pkpos, 0), c.relreplident
    FROM pg_attribute a
    JOIN pg_class c ON c.oid = a.attrelid AND c.relkind = 'r' AND c.relpersistence = 'p'
    JOIN pg_namespace ns ON ns.oid = c.relnamespace AND ns.nspname = 'public'
    LEFT JOIN LATERAL (
        SELECT x.ord AS pkpos
        FROM pg_index i, unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
        WHERE i.indrelid = a.attrelid AND i.indisprimary AND x.attnum = a.attnum
    ) k ON true
    WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relname NOT LIKE 'syzy\_%';
END $$;

-- syzy_type_mods extracts a format_type() modifier list — "numeric(10,2)" → {10,2},
-- anything without an all-numeric modifier → {}.
CREATE OR REPLACE FUNCTION syzy_type_mods(t text) RETURNS int[]
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog AS $$
    SELECT CASE WHEN t ~ '^[^(]+\([0-9]+(,[0-9]+)?\)$'
                THEN string_to_array(substring(t from '\(([0-9,]+)\)'), ',')::int[]
                ELSE '{}'::int[] END
$$;

-- syzy_type_widens: every value of v_from is also a value of v_to. The twin of
-- typeWidens() in ddl_catalog.go — the two are held identical by a test that
-- runs the same pairs through both.
CREATE OR REPLACE FUNCTION syzy_type_widens(v_from text, v_to text) RETURNS boolean
LANGUAGE plpgsql IMMUTABLE SET search_path = pg_catalog, public AS $$
DECLARE
    fb   text := split_part(v_from, '(', 1);
    tb   text := split_part(v_to, '(', 1);
    ints constant text[] := ARRAY['smallint', 'integer', 'bigint'];
    fm   int[];
    tm   int[];
BEGIN
    IF fb <> tb THEN
        RETURN (fb = 'character varying' AND tb = 'text')
            OR (fb = 'real' AND tb = 'double precision')
            OR COALESCE(array_position(ints, tb) > array_position(ints, fb), false);
    END IF;
    IF fb NOT IN ('character varying', 'numeric') THEN
        RETURN false;
    END IF;
    fm := syzy_type_mods(v_from);
    tm := syzy_type_mods(v_to);
    IF array_length(tm, 1) IS NULL THEN
        RETURN true;  -- an unconstrained target holds every constrained value
    END IF;
    -- An unmodified source (array_length → NULL, e.g. bare "numeric") is not a
    -- subset of a constrained target, so these must land on false, not NULL:
    -- the caller treats NULL as "no rule matched" and would admit the change.
    IF fb = 'character varying' THEN
        RETURN COALESCE(array_length(fm, 1) = 1 AND array_length(tm, 1) = 1 AND tm[1] > fm[1], false);
    END IF;
    RETURN COALESCE(array_length(fm, 1) = 2 AND array_length(tm, 1) = 2
                    AND tm[2] = fm[2] AND tm[1] >= fm[1], false);
END $$;

-- syzy_ddl_admit_table rejects a new table whose shape can never replicate. Its
-- twin in the sidecar is buildCreateTableOp/rejectUnreplicable, which stays as
-- the post-commit floor.
CREATE OR REPLACE FUNCTION syzy_ddl_admit_table(rel oid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    tname text := rel::regclass::text;
    c     record;
    bad   text;
BEGIN
    SELECT relkind, relispartition INTO c FROM pg_class WHERE oid = rel;
    IF c.relkind = 'p' OR c.relispartition THEN
        RAISE EXCEPTION 'syzy: CREATE TABLE %: partitioned tables are not supported', tname
            USING ERRCODE = 'feature_not_supported';
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_index WHERE indrelid = rel AND indisprimary) THEN
        RAISE EXCEPTION 'syzy: CREATE TABLE %: a replicated table requires a PRIMARY KEY — rows are identified by it, and without one no write could be merged', tname
            USING ERRCODE = 'feature_not_supported';
    END IF;
    -- A column of a user-defined type (enum, domain, composite, extension type)
    -- would replicate as text into a type the receiving node may not have — and
    -- an enum that later gains a value on one node only would fail apply there
    -- forever. Built-in types (and arrays of them) live in pg_catalog.
    SELECT string_agg(format('%I %s', a.attname, format_type(a.atttypid, a.atttypmod)), ', ')
      INTO bad
      FROM pg_attribute a
      JOIN pg_type t ON t.oid = a.atttypid
      JOIN pg_namespace tn ON tn.oid = t.typnamespace
     WHERE a.attrelid = rel AND a.attnum > 0 AND NOT a.attisdropped
       AND tn.nspname <> 'pg_catalog'
       -- syzy_counter is the one non-built-in type that replicates: it is a
       -- bigint domain every node installs, and it is how a counter column is
       -- declared (sql/counter.sql).
       AND NOT (tn.nspname = 'public' AND t.typname = 'syzy_counter');
    IF bad IS NOT NULL THEN
        RAISE EXCEPTION 'syzy: CREATE TABLE %: column(s) % use a user-defined type; only built-in types replicate', tname, bad
            USING ERRCODE = 'feature_not_supported';
    END IF;
END $$;

-- syzy_ddl_admit_alter rejects — pre-commit, so the user's transaction rolls
-- back cleanly — an ALTER that RESTRICTS a replicated table. A row written on a
-- peer before it applied the change is already in flight carrying values that
-- were legal under the old shape, so a receiver that had tightened the column
-- could never apply it and would halt forever. Relaxations (widening a type,
-- DROP NOT NULL, default changes) replicate and are admitted. The same rules
-- run post-commit in classifyColumnChange(), which stays as the floor for a
-- node whose DDL support was installed after the fact.
CREATE OR REPLACE FUNCTION syzy_ddl_admit_alter(rel oid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    tname text := rel::regclass::text;
    live  record;
    prior record;
BEGIN
    FOR live IN
        SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS typename,
               a.attnotnull AS is_notnull, a.attgenerated <> '' AS is_generated,
               a.attidentity AS identity, COALESCE(k.pkpos, 0) AS pkpos,
               COALESCE(pg_get_expr(d.adbin, d.adrelid), '') AS def
        FROM pg_attribute a
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        LEFT JOIN LATERAL (
            SELECT x.ord AS pkpos
            FROM pg_index i, unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
            WHERE i.indrelid = a.attrelid AND i.indisprimary AND x.attnum = a.attnum
        ) k ON true
        WHERE a.attrelid = rel AND a.attnum > 0 AND NOT a.attisdropped
    LOOP
        SELECT * INTO prior FROM syzy_ddl_prior p
        WHERE p.pid = pg_backend_pid() AND p.relid = rel AND p.attname = live.attname;
        IF NOT FOUND THEN
            -- ADD COLUMN. Its values are minted per node for existing rows, so an
            -- auto-increment column cannot be added to a table that already has any.
            IF live.identity <> '' OR live.def LIKE 'nextval(%' THEN
                RAISE EXCEPTION 'syzy: ALTER TABLE %: ADD COLUMN "%" is auto-increment (serial/identity); it mints divergent per-node values for existing rows', tname, live.attname
                    USING ERRCODE = 'feature_not_supported';
            END IF;
            CONTINUE;
        END IF;
        IF live.is_generated <> prior.is_generated THEN
            RAISE EXCEPTION 'syzy: ALTER TABLE %: adding or dropping a GENERATED expression on column "%" recomputes every row from a node-local evaluation and is not supported', tname, live.attname
                USING ERRCODE = 'feature_not_supported';
        END IF;
        IF live.identity <> prior.identity THEN
            RAISE EXCEPTION 'syzy: ALTER TABLE %: changing IDENTITY on column "%" mints divergent per-node values and is not supported', tname, live.attname
                USING ERRCODE = 'feature_not_supported';
        END IF;
        IF (live.pkpos > 0) <> (prior.pkpos > 0) THEN
            RAISE EXCEPTION 'syzy: ALTER TABLE %: changing PRIMARY KEY membership of column "%" changes row identity and is not supported', tname, live.attname
                USING ERRCODE = 'feature_not_supported';
        END IF;
        IF live.is_notnull AND NOT prior.is_notnull THEN
            RAISE EXCEPTION 'syzy: ALTER TABLE %: SET NOT NULL on column "%" cannot replicate — a NULL written on a peer before it applied the change is already in flight and would fail apply there permanently; declare the column NOT NULL when it is created', tname, live.attname
                USING ERRCODE = 'feature_not_supported';
        END IF;
        IF live.typename <> prior.typename AND NOT syzy_type_widens(prior.typename, live.typename) THEN
            RAISE EXCEPTION 'syzy: ALTER TABLE %: changing column "%" from % to % is not a widening conversion — values already in flight under the old type could not be applied', tname, live.attname, prior.typename, live.typename
                USING ERRCODE = 'feature_not_supported';
        END IF;
        IF live.def LIKE 'nextval(%' THEN
            RAISE EXCEPTION 'syzy: ALTER TABLE %: DEFAULT nextval(...) on column "%" names a node-local sequence and cannot replicate', tname, live.attname
                USING ERRCODE = 'feature_not_supported';
        END IF;
    END LOOP;
    IF EXISTS (
        SELECT 1 FROM syzy_ddl_prior p
        WHERE p.pid = pg_backend_pid() AND p.relid = rel AND p.pkpos > 0
          AND NOT EXISTS (
            SELECT 1 FROM pg_attribute a
            WHERE a.attrelid = rel AND a.attname = p.attname AND NOT a.attisdropped)
    ) THEN
        RAISE EXCEPTION 'syzy: ALTER TABLE %: dropping a PRIMARY KEY column changes row identity and is not supported', tname
            USING ERRCODE = 'feature_not_supported';
    END IF;
END $$;

-- syzy_ddl_admit_cells enforces the merge-semantics rules for the cell clock
-- group and counter columns (docs/postgres.md §8), and installs the physical
-- capability they need.
--
-- REPLICA IDENTITY FULL is the cell-group opt-in: it is exactly the capability
-- per-column merge requires (capture diffs the old tuple against the new to
-- learn which columns a transaction actually changed), so the physical setting
-- and the merge rule cannot drift apart. A counter column implies the cell
-- group, so a table that declares one has FULL set here — in the same
-- transaction as the DDL that declared it, before any row can be written.
--
-- Its twins in the sidecar are classifyColumnChange / buildAlterTableOps, which
-- stay as the post-commit floor for a node whose DDL support was installed late.
CREATE OR REPLACE FUNCTION syzy_ddl_admit_cells(rel oid) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    tname     text := rel::regclass::text;
    c         record;
    counters  boolean := false;
    live_ri   "char";
    prior_ri  "char";
BEGIN
    SELECT relreplident INTO live_ri FROM pg_class WHERE oid = rel;
    SELECT max(p.replident) INTO prior_ri FROM syzy_ddl_prior p
     WHERE p.pid = pg_backend_pid() AND p.relid = rel;

    -- Counter columns merge by summation: every contribution has to be a real
    -- number the receiver can add, on a cell that no other rule may overwrite.
    FOR c IN
        SELECT a.attname, a.attnotnull, a.attgenerated <> '' AS is_generated,
               a.attidentity AS identity, COALESCE(k.pkpos, 0) AS pkpos
        FROM pg_attribute a
        JOIN pg_type t ON t.oid = a.atttypid
        JOIN pg_namespace tn ON tn.oid = t.typnamespace
        LEFT JOIN LATERAL (
            SELECT x.ord AS pkpos
            FROM pg_index i, unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
            WHERE i.indrelid = a.attrelid AND i.indisprimary AND x.attnum = a.attnum
        ) k ON true
        WHERE a.attrelid = rel AND a.attnum > 0 AND NOT a.attisdropped
          AND tn.nspname = 'public' AND t.typname = 'syzy_counter'
    LOOP
        counters := true;
        IF NOT c.attnotnull THEN
            RAISE EXCEPTION 'syzy: %: counter column "%" must be NOT NULL — a NULL cell has no value to sum contributions into', tname, c.attname
                USING ERRCODE = 'feature_not_supported';
        END IF;
        IF c.pkpos > 0 THEN
            RAISE EXCEPTION 'syzy: %: counter column "%" cannot be part of the PRIMARY KEY — row identity must not change when a contribution is summed in', tname, c.attname
                USING ERRCODE = 'feature_not_supported';
        END IF;
        IF c.is_generated OR c.identity <> '' THEN
            RAISE EXCEPTION 'syzy: %: counter column "%" cannot be GENERATED or an IDENTITY column — its value is the sum of replicated contributions, not a node-local expression', tname, c.attname
                USING ERRCODE = 'feature_not_supported';
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_index i, unnest(i.indkey) AS x(attnum)
            JOIN pg_attribute a ON a.attrelid = rel AND a.attnum = x.attnum
            WHERE i.indrelid = rel AND i.indisunique AND a.attname = c.attname
        ) OR EXISTS (
            -- Coordinated keys carry no physical index (coordinated.sql); their
            -- member columns are named in the accumulating trigger's arguments.
            SELECT 1 FROM pg_trigger tg
            WHERE tg.tgrelid = rel AND NOT tg.tgisinternal
              AND tg.tgname LIKE 'syzy\_coord\_accum\_%'
              AND pg_get_triggerdef(tg.oid) LIKE '%' || quote_literal(c.attname) || '%'
        ) THEN
            RAISE EXCEPTION 'syzy: %: counter column "%" cannot be part of a UNIQUE key — concurrent contributions sum to a value no writer reserved', tname, c.attname
                USING ERRCODE = 'feature_not_supported';
        END IF;
    END LOOP;

    IF counters AND live_ri <> 'f' THEN
        IF prior_ri = 'f' THEN
            -- The command turned the opt-in off under the counters' feet.
            RAISE EXCEPTION 'syzy: %: counter columns merge per column and require REPLICA IDENTITY FULL; drop the counter columns before leaving the cell clock group', tname
                USING ERRCODE = 'feature_not_supported';
        END IF;
        -- The table just declared its first counter column: give it the cell
        -- clock group now, inside this transaction, so no row is ever written
        -- to a counter column under whole-row merge.
        PERFORM set_config('syzy.internal', 'on', true);
        EXECUTE format('ALTER TABLE %s REPLICA IDENTITY FULL', tname);
        PERFORM set_config('syzy.internal', 'off', true);
        live_ri := 'f';
    END IF;

    IF live_ri = 'f' AND EXISTS (
        SELECT 1 FROM pg_trigger tg
        WHERE tg.tgrelid = rel AND NOT tg.tgisinternal
          AND tg.tgname LIKE 'syzy\_coord\_accum\_%'
          AND tg.tgnargs > 3
    ) THEN
        RAISE EXCEPTION 'syzy: %: a composite coordinated (NOT NULL UNIQUE) key cannot use the cell clock group — per-column merge could assemble a row from writes that were never reserved together', tname
            USING ERRCODE = 'feature_not_supported';
    END IF;
END $$;

-- ddl_command_end fires once per top-level DDL command (CREATE/ALTER); it does
-- NOT expose per-sub-object rows (ALTER TABLE t ADD COLUMN a is objid=t,
-- objsubid=0), so the consumer reconstructs the column change by diffing the
-- catalog — the descriptor only names the table + command_tag.
CREATE OR REPLACE FUNCTION syzy_ddl_command_end() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    r record;
    persist "char";
BEGIN
    IF current_setting('syzy.internal', true) = 'on' THEN RETURN; END IF;
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands() LOOP
        -- Only PERMANENT relations are in scope at all. A TEMP table is
        -- session-local and an UNLOGGED table is not WAL-logged, so logical
        -- decoding never sees either's DML. Checked first, so a harmless
        -- CREATE TEMP TABLE … AS is not caught by the rejection below.
        SELECT relpersistence INTO persist FROM pg_class WHERE oid = r.objid;
        IF persist IS NOT NULL AND persist <> 'p' THEN
            CONTINUE;
        END IF;
        -- Admission (§6 G): a permanent CREATE TABLE AS / SELECT INTO / MATERIALIZED
        -- VIEW materializes a node-local query result that cannot replicate. Reject
        -- it PRE-COMMIT so the user txn rolls back cleanly, instead of committing and
        -- forcing the sidecar to halt schema-unhealthy post-commit.
        IF r.command_tag IN ('CREATE TABLE AS', 'SELECT INTO', 'CREATE MATERIALIZED VIEW') THEN
            RAISE EXCEPTION 'syzy: % is not replicable (node-local query result); use CREATE TABLE then INSERT ... SELECT', r.command_tag
                USING ERRCODE = 'feature_not_supported';
        END IF;
        -- Replication scope. Only these commands, on a table/view/index in the
        -- replicated schema, become intent rows. EVERYTHING else — extensions,
        -- functions, procedures, triggers, types, schemas, standalone sequences,
        -- GRANT, COMMENT, and any object outside public — is local to this node:
        -- no intent row, so capture never sees it and the node can never halt on
        -- it. (A trigger's or function's EFFECTS still replicate: they run on the
        -- originator and their row changes are captured as ordinary DML.)
        IF r.command_tag NOT IN ('CREATE TABLE', 'ALTER TABLE', 'CREATE INDEX', 'CREATE VIEW')
           OR COALESCE(r.schema_name, '') <> 'public' THEN
            CONTINUE;
        END IF;
        -- Admission (§6 G): shapes that cannot replicate at all, and column
        -- changes that RESTRICT the table (judged against the pre-command
        -- snapshot) — both rejected before they can commit.
        IF r.command_tag = 'CREATE TABLE' THEN
            PERFORM syzy_ddl_admit_table(r.objid);
        END IF;
        IF r.command_tag = 'ALTER TABLE' AND r.object_type = 'table' THEN
            PERFORM syzy_ddl_admit_alter(r.objid);
        END IF;
        -- Merge semantics (§8): counter-column rules, and the REPLICA IDENTITY
        -- FULL the cell clock group runs on.
        IF r.command_tag IN ('CREATE TABLE', 'ALTER TABLE') AND r.object_type = 'table' THEN
            PERFORM syzy_ddl_admit_cells(r.objid);
        END IF;
        INSERT INTO syzy_ddl_intent
          (txid, ordinal, command_tag, object_type, classid, objid, objsubid,
           schema_name, object_identity, is_drop, audit_query)
        VALUES
          (txid_current(), syzy_ddl_next_ordinal(), r.command_tag, r.object_type,
           r.classid, r.objid, r.objsubid, r.schema_name, r.object_identity,
           false, current_query());
    END LOOP;
END $$;

-- sql_drop fires for dropped objects; record only the directly-dropped ones
-- (r.original), not their cascade dependents — the consumer re-derives drops of
-- dependents from the catalog.
CREATE OR REPLACE FUNCTION syzy_ddl_sql_drop() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE r record;
BEGIN
    IF current_setting('syzy.internal', true) = 'on' THEN RETURN; END IF;
    FOR r IN SELECT * FROM pg_event_trigger_dropped_objects()
             WHERE original AND NOT is_temporary
               AND object_type IN ('table', 'index', 'view')
               AND COALESCE(schema_name, '') = 'public' LOOP
        INSERT INTO syzy_ddl_intent
          (txid, ordinal, command_tag, object_type, classid, objid, objsubid,
           schema_name, object_identity, is_drop, audit_query)
        VALUES
          (txid_current(), syzy_ddl_next_ordinal(), 'DROP', r.object_type,
           r.classid, r.objid, r.objsubid, r.schema_name, r.object_identity,
           true, current_query());
    END LOOP;
END $$;

-- ENABLE ORIGIN (the default fires-state): the triggers fire for user DDL but
-- are silent when the sidecar applies peer DDL under session_replication_role=
-- replica, so applied DDL writes no intent rows and is never re-captured.
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'syzy_ddl_snapshot') THEN
        CREATE EVENT TRIGGER syzy_ddl_snapshot ON ddl_command_start
            EXECUTE FUNCTION syzy_ddl_snapshot();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'syzy_ddl_end') THEN
        CREATE EVENT TRIGGER syzy_ddl_end ON ddl_command_end
            EXECUTE FUNCTION syzy_ddl_command_end();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'syzy_ddl_drop') THEN
        CREATE EVENT TRIGGER syzy_ddl_drop ON sql_drop
            EXECUTE FUNCTION syzy_ddl_sql_drop();
    END IF;
END $$;
