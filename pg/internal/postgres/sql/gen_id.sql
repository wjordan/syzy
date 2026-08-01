-- gen_id(table): the cross-engine node-disjoint id generator. The SQLite
-- engine rewrites bare INTEGER PRIMARY KEY columns to
-- `DEFAULT (gen_id('<table>'))`, so a SQLite-authored schema applied here
-- references gen_id — this is its Postgres twin with the same id layout:
-- a random 30-bit partition (probed empty) in the high bits, a 33-bit
-- counter in the low bits, partition 0 reserved (its keyspace starts at 1,
-- colliding with hand-inserted starter values).
--
-- State lives in syzy_gen_id, one row per table: the partition is chosen
-- once and the counter persists across restarts (SQLite re-probes per
-- process; both satisfy the same contract — ids never repeat and never
-- collide across writers). Row-level locking on the UPDATE serializes
-- concurrent local inserts. The table is engine-local plumbing: untracked
-- by the catalog, so capture drops its DML.

CREATE TABLE IF NOT EXISTS public.syzy_gen_id (
    table_name text PRIMARY KEY,
    partition  bigint NOT NULL,
    counter    bigint NOT NULL DEFAULT 0
);

CREATE OR REPLACE FUNCTION public.gen_id(p_table text) RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
    v_counter   bigint;
    v_partition bigint;
    v_pkcol     name;
    v_occupied  boolean;
    i           int;
BEGIN
    -- Fast path: state exists; bump under the row lock.
    UPDATE public.syzy_gen_id SET counter = counter + 1
        WHERE table_name = p_table
        RETURNING partition, counter INTO v_partition, v_counter;
    IF NOT FOUND THEN
        -- First call for this table: find its single-column PK, probe random
        -- partitions for an empty id range, seed the state row (losing a
        -- creation race is fine — ON CONFLICT defers to the winner), retry
        -- the bump.
        SELECT a.attname INTO v_pkcol
        FROM pg_index x
        JOIN pg_attribute a ON a.attrelid = x.indrelid AND a.attnum = x.indkey[0]
        WHERE x.indrelid = format('public.%I', p_table)::regclass AND x.indisprimary AND x.indnkeyatts = 1;
        IF v_pkcol IS NULL THEN
            RAISE EXCEPTION 'gen_id: table % needs a single-column primary key', p_table;
        END IF;
        FOR i IN 1..16 LOOP
            v_partition := 1 + floor(random() * ((1::bigint << 30) - 1))::bigint;
            EXECUTE format('SELECT EXISTS (SELECT 1 FROM public.%I WHERE %I >= $1 AND %I < $2)',
                           p_table, v_pkcol, v_pkcol)
                INTO v_occupied
                USING v_partition << 33, (v_partition + 1) << 33;
            IF NOT v_occupied THEN
                INSERT INTO public.syzy_gen_id (table_name, partition)
                    VALUES (p_table, v_partition)
                    ON CONFLICT (table_name) DO NOTHING;
                EXIT;
            END IF;
            IF i = 16 THEN
                RAISE EXCEPTION 'gen_id: no free partition for % after 16 probes', p_table;
            END IF;
        END LOOP;
        UPDATE public.syzy_gen_id SET counter = counter + 1
            WHERE table_name = p_table
            RETURNING partition, counter INTO v_partition, v_counter;
    END IF;
    IF v_counter > ((1::bigint << 33) - 1) THEN
        RAISE EXCEPTION 'gen_id: counter exhausted in partition %', v_partition;
    END IF;
    RETURN (v_partition << 33) | v_counter;
END $$;
