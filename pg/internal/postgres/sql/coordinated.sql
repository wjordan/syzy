-- Coordinated unique keys (§8), v1: leaseholder-routed writes. Every node
-- holds the physical UNIQUE index for a coordinated key; a gate trigger on
-- each coordinated table admits INSERTs (and key-column UPDATEs) only while
-- this node is the cluster's unique-write leaseholder. The sidecar's lease
-- loop maintains the single gate row; the trigger reads it per statement.
--
-- The gate is a REGULAR (ENABLE ORIGIN) trigger, so the apply session
-- (session_replication_role = replica) bypasses it — replicated rows land on
-- followers regardless of gate state, and their physical index is the
-- last-resort backstop (a genuine conflict there quarantines, it never
-- diverges).
--
-- syzy_unique_gate is engine-local plumbing: untracked by the catalog, so its
-- DML is dropped by capture like syzy_ddl_intent's own rows.

CREATE TABLE IF NOT EXISTS public.syzy_unique_gate (
    id         boolean PRIMARY KEY DEFAULT true CHECK (id),
    open       boolean NOT NULL DEFAULT false,
    expires_at timestamptz NOT NULL DEFAULT '-infinity'
);
INSERT INTO public.syzy_unique_gate (id) VALUES (true) ON CONFLICT DO NOTHING;

CREATE OR REPLACE FUNCTION public.syzy_coordinated_gate() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.syzy_unique_gate WHERE open AND expires_at > now()) THEN
        RAISE EXCEPTION 'syzy: not the unique-write leaseholder; coordinated-key writes must run on the leaseholder node'
            USING ERRCODE = '55006'; -- object_not_in_prerequisite_state
    END IF;
    RETURN NEW;
END $$;
