DROP VIEW IF EXISTS public.address_lookup;
CREATE OR REPLACE VIEW public.address_lookup AS
SELECT uprn,
       parent_uprn,
       usrn,
       organisation_name,
       line1,
       line2,
       line3,
       subdivision,
       country_code,
       local_custodian_code,
       language,
       blpu_state,
       logical_status,
       posttown,
       postcode,
       location,
       pobox_number,
       local_authority,
       address_lookup_ft_col
FROM __schema__.address_lookup;
GRANT SELECT ON public.address_lookup TO addresslookupreader;
UPDATE public.address_lookup_status SET status = 'finalised' WHERE schema_name = '__schema__';
