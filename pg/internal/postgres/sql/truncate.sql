-- TRUNCATE has no row images to merge. Every replicated table gets a
-- statement trigger that rejects it before the transaction can commit.
CREATE OR REPLACE FUNCTION public.syzy_reject_truncate() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
BEGIN
    RAISE EXCEPTION 'syzy: TRUNCATE is not replicable; use DELETE so row tombstones replicate'
        USING ERRCODE = 'feature_not_supported';
END $$;

CREATE OR REPLACE FUNCTION public.syzy_install_truncate_guard(target oid) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
DECLARE
    prior_internal text;
BEGIN
    IF EXISTS (SELECT 1 FROM pg_trigger
               WHERE tgrelid = target
                 AND tgname = 'syzy_reject_truncate'
                 AND NOT tgisinternal) THEN
        RETURN;
    END IF;

    prior_internal := current_setting('syzy.internal', true);
    PERFORM set_config('syzy.internal', 'on', true);
    EXECUTE format(
        'CREATE TRIGGER syzy_reject_truncate BEFORE TRUNCATE ON %s FOR EACH STATEMENT EXECUTE FUNCTION public.syzy_reject_truncate()',
        target::regclass
    );
    PERFORM set_config('syzy.internal', COALESCE(NULLIF(prior_internal, ''), 'off'), true);
END $$;

REVOKE ALL ON FUNCTION public.syzy_install_truncate_guard(oid) FROM PUBLIC;
