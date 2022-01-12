CREATE OR REPLACE FUNCTION create_address_lookup_view(the_schema_name varchar)
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS
$$
DECLARE
BEGIN
    EXECUTE format('SET search_path = %s', the_schema_name);

    UPDATE public.address_lookup_status
    SET status    = 'creating_view',
        timestamp = now()
    WHERE schema_name = the_schema_name;

    DROP VIEW IF EXISTS address_lookup_uppercase;

    CREATE VIEW address_lookup_uppercase AS
    SELECT b.uprn                             AS uprn,
           b.parent_uprn                      AS parent_uprn,
           asd.usrn                           AS usrn,
           UPPER(d.organisation_name)         AS organisation_name,
           UPPER(d.po_box_number)             AS po_box_number,
           UPPER(d.sub_building_name)         AS sub_building_name,
           UPPER(d.building_name)             AS building_name,
           d.building_number                  AS building_number,
           UPPER(d.dependent_thoroughfare)    AS dependent_thoroughfare,
           UPPER(d.thoroughfare)              AS thoroughfare,
           UPPER(d.double_dependent_locality) AS double_dependent_locality,
           UPPER(d.dependent_locality)        AS dependent_locality,
           UPPER(b.country)                   AS country,
           b.local_custodian_code             AS local_custodian_code,
           UPPER(l.language)                  AS "language",
           UPPER(d.postcode)                  AS postcode,
           b.blpu_state                       AS blpu_state,
           b.logical_status                   AS logical_status,
           UPPER(d.post_town)                 AS post_town,
           b.latitude                         AS latitude,
           b.longitude                        AS longitude,
           asd.administrative_area            AS administrative_area
    FROM abp_delivery_point d
             JOIN abp_blpu b ON b.uprn = d.uprn
             JOIN abp_lpi l ON l.uprn = b.uprn AND l.logical_status = 1
             JOIN abp_street_descriptor asd ON l.usrn = asd.usrn AND l.language = asd.language

    DROP MATERIALIZED VIEW IF EXISTS address_lookup;

    CREATE MATERIALIZED VIEW address_lookup AS
    SELECT uprn                                                                                              AS uprn,
           parent_uprn                                                                                       AS parent_uprn,
           usrn                                                                                              AS usrn,
           organisation_name                                                                                 AS organisation_name,
           CASE
               WHEN NULLIF(BTRIM(po_box_number::text), '') IS NOT NULL THEN
                   'PO BOX ' || BTRIM(po_box_number::text)
               ELSE
                   array_to_string(ARRAY[NULLIF(btrim(sub_building_name::text), ''),
                                   NULLIF(btrim(building_name::text), '') ], ', '::text)
           END                                                                                               AS line1,
           CASE
               WHEN NULLIF(BTRIM(po_box_number::text), '') IS NOT NULL THEN ''
               ELSE
                   array_to_string(
                           ARRAY[NULLIF(btrim(''::text || building_number), ''),
                           NULLIF(btrim(dependent_thoroughfare::text), ''), NULLIF(btrim(thoroughfare::text), '') ],
                           ' '::text)
           END                                                                                               AS line2,
           array_to_string(ARRAY[NULLIF(btrim(double_dependent_locality::text), ''),
                           NULLIF(btrim(dependent_locality::text), '') ],
                           ' '::text) AS line3,
           CASE
               WHEN country::text = 'S'::text THEN 'GB-SCT'::text
               WHEN country::text = 'E'::text THEN 'GB-ENG'::text
               WHEN country::text = 'W'::text THEN 'GB-WLS'::text
               WHEN country::text = 'N'::text THEN 'GB-NIR'::text
               ELSE NULL::text
           END                                                                                               AS subdivision,
           CASE
               WHEN country::text = 'S'::text THEN 'GB'::text
               WHEN country::text = 'E'::text THEN 'GB'::text
               WHEN country::text = 'W'::text THEN 'GB'::text
               WHEN country::text = 'N'::text THEN 'GB'::text
               WHEN country::text = 'L'::text AND "substring"(postcode::text, 1, 1) = 'G'::text THEN 'GG'::text
               WHEN country::text = 'L'::text AND "substring"(postcode::text, 1, 1) = 'J'::text THEN 'JE'::text
               WHEN country::text = 'M'::text THEN 'IM'::text
               ELSE 'NOT_FOUND'::text
           END                                                                                                   AS country_code,
           local_custodian_code                                                                                  AS local_custodian_code,
           CASE
               WHEN "language"::text = 'ENG'::text THEN 'en'::text
               WHEN "language"::text = 'CYM'::text THEN 'cy'::text
               ELSE NULL::text
           END                                                                                                   AS "language",
           blpu_state                                                                                            AS blpu_state,
           logical_status                                                                                        AS logical_status,
           post_town                                                                                             AS posttown,
           upper(regexp_replace(postcode::text, '[ \\t]+'::text, ' '::text))                                     AS postcode,
           concat(latitude, ',', longitude)                                                                      AS location,
           NULLIF(BTRIM(po_box_number::text), '')                                                                AS pobox_number,
           administrative_area                                                                                   AS local_authority,
           to_tsvector('english'::regconfig, array_to_string(
                   ARRAY [
                       NULLIF(btrim(sub_building_name::text), ''),
                       NULLIF(btrim(building_name::text), ''),
                       NULLIF(btrim(building_number::text), ''),
                       NULLIF(btrim(dependent_thoroughfare::text), ''),
                       NULLIF(btrim(thoroughfare::text), ''),
                       NULLIF(btrim(post_town::text), ''),
                       NULLIF(btrim(double_dependent_locality::text), ''),
                       NULLIF(btrim(dependent_locality::text), ''),
                       NULLIF(btrim(administrative_area::text), ''),
                       NULLIF(btrim(po_box_number::text), ''),
                       NULLIF(btrim(organisation_name::text), '')],
                   ' '::text))                                                                                   AS address_lookup_ft_col
    FROM address_lookup_uppercase
    WHERE "language" = 'ENG';

    UPDATE public.address_lookup_status
    SET status    = 'view_created',
        timestamp = now()
    WHERE schema_name = the_schema_name;

    CREATE INDEX IF NOT EXISTS address_lookup_ft_col_idx
        ON address_lookup USING gin (address_lookup_ft_col);

    UPDATE public.address_lookup_status
    SET status    = 'gin_index_created',
        timestamp = now()
    WHERE schema_name = the_schema_name;

    CREATE INDEX IF NOT EXISTS address_lookup_postcode_idx
        ON address_lookup (postcode);

    UPDATE public.address_lookup_status
    SET status    = 'postcode_index_created',
        timestamp = now()
    WHERE schema_name = the_schema_name;

    CREATE INDEX address_lookup_outcode_idx
        ON address_lookup (substring(postcode, 0, position(' '::text IN postcode)));

    UPDATE public.address_lookup_status
    SET status    = 'outcodecode_index_created',
        timestamp = now()
    WHERE schema_name = the_schema_name;

    CREATE INDEX address_lookup_town_idx
        ON address_lookup (posttown);

    UPDATE public.address_lookup_status
    SET status    = 'posttown_index_created',
        timestamp = now()
    WHERE schema_name = the_schema_name;

    CREATE INDEX IF NOT EXISTS address_lookup_uprn_idx
        ON address_lookup (uprn);

    UPDATE public.address_lookup_status
    SET status    = 'uprn_index_created',
        timestamp = now()
    WHERE schema_name = the_schema_name;

    UPDATE public.address_lookup_status
    SET status    = 'public_view_created',
        timestamp = now()
    WHERE schema_name = the_schema_name;

    UPDATE public.address_lookup_status
    SET status    = 'completed',
        timestamp = now()
    WHERE schema_name = the_schema_name;

    RETURN TRUE;
EXCEPTION
    WHEN OTHERS THEN
        UPDATE public.address_lookup_status
        SET status    = 'errored',
            error_message = SQLERRM,
            timestamp = now()
        WHERE schema_name = the_schema_name;
END;
$$
