DROP VIEW IF EXISTS public.address_lookup;
CREATE OR REPLACE VIEW public.address_lookup AS SELECT * FROM __schema__.address_lookup;
GRANT SELECT ON public.address_lookup TO addresslookupreader;
UPDATE public.address_lookup_status SET status = 'finalised' WHERE schema_name = '__schema__';