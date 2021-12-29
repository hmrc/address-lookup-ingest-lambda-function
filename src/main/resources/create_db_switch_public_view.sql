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
       country_code                 AS countrycode,
       local_custodian_code,
       local_custodian_code         AS localcustodiancode,
       language,
       blpu_state,
       blpu_state                   AS blpustate,
       logical_status,
       logical_status               AS logicalstatus,
       posttown,
       postcode,
       location,
       pobox_number,
       pobox_number                 AS poboxnumber,
       local_authority,
       local_authority              AS localAuthority,
       address_lookup_ft_col
FROM __schema__.address_lookup;
GRANT SELECT ON public.address_lookup TO addresslookupreader;
UPDATE public.address_lookup_status SET status = 'finalised' WHERE schema_name = '__schema__';
