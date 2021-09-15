DROP SCHEMA IF EXISTS __schema__ CASCADE;
DELETE FROM public.address_lookup_status
WHERE schema_name = '__schema__';