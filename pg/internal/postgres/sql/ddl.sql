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
    n := COALESCE(current_setting('syzy.ddl_ordinal', true), '0')::int;
    PERFORM set_config('syzy.ddl_ordinal', (n + 1)::text, true);  -- true = txn-local
    RETURN n;
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
        -- Replication scope: only PERMANENT relations. A TEMP table is
        -- session-local and an UNLOGGED table is not WAL-logged, so logical
        -- decoding never sees either's DML — skip their DDL entirely (no intent
        -- row), rather than capture it and later (wrongly) halt as unreplicable.
        SELECT relpersistence INTO persist FROM pg_class WHERE oid = r.objid;
        IF persist IS NOT NULL AND persist <> 'p' THEN
            CONTINUE;
        END IF;
        -- Admission (§6 G): a permanent CREATE TABLE AS / SELECT INTO materializes
        -- a node-local query result that cannot replicate. Reject it PRE-COMMIT so
        -- the user txn rolls back cleanly, instead of committing and forcing the
        -- sidecar to halt schema-unhealthy post-commit.
        IF r.command_tag IN ('CREATE TABLE AS', 'SELECT INTO') THEN
            RAISE EXCEPTION 'syzy: % is not replicable (node-local query result); use CREATE TABLE then INSERT ... SELECT', r.command_tag
                USING ERRCODE = 'feature_not_supported';
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
    FOR r IN SELECT * FROM pg_event_trigger_dropped_objects() WHERE original LOOP
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
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'syzy_ddl_end') THEN
        CREATE EVENT TRIGGER syzy_ddl_end ON ddl_command_end
            EXECUTE FUNCTION syzy_ddl_command_end();
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = 'syzy_ddl_drop') THEN
        CREATE EVENT TRIGGER syzy_ddl_drop ON sql_drop
            EXECUTE FUNCTION syzy_ddl_sql_drop();
    END IF;
END $$;
