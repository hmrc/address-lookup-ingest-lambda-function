DROP MATERIALIZED VIEW IF EXISTS __schema__.address_lookup;

CREATE MATERIALIZED VIEW __schema__.address_lookup AS
SELECT array_to_string(ARRAY [btrim(d.sub_building_name::text), btrim(d.building_name::text)], ', '::text) AS line1,
       array_to_string(
               ARRAY [btrim(''::text || d.building_number), btrim(d.dependent_thoroughfare::text), btrim(d.thoroughfare::text)],
               ' '::text)                                                                                  AS line2,
       array_to_string(ARRAY [btrim(d.double_dependent_locality::text), btrim(d.dependent_locality::text)],
                       ' '::text)                                                                          AS line3,
       CASE
           WHEN b.country::text = 'S'::text THEN 'GB-SCT'::text
           WHEN b.country::text = 'E'::text THEN 'GB-ENG'::text
           WHEN b.country::text = 'W'::text THEN 'GB-WLS'::text
           WHEN b.country::text = 'N'::text THEN 'GB-NIR'::text
           ELSE NULL::text
           END                                                                                             AS subdivision,
       CASE
           WHEN b.country::text = 'S'::text THEN 'UK'::text
           WHEN b.country::text = 'E'::text THEN 'UK'::text
           WHEN b.country::text = 'W'::text THEN 'UK'::text
           WHEN b.country::text = 'N'::text THEN 'UK'::text
           WHEN b.country::text = 'L'::text AND "substring"(d.postcode::text, 1, 1) = 'G'::text THEN 'GG'::text
           WHEN b.country::text = 'L'::text AND "substring"(d.postcode::text, 1, 1) = 'J'::text THEN 'JE'::text
           WHEN b.country::text = 'M'::text THEN 'IM'::text
           ELSE 'NOT_FOUND'::text
           END                                                                                             AS countrycode,
       b.local_custodian_code                                                                              AS localcustodiancode,
       CASE
           WHEN lower(l.language::text) = 'eng'::text THEN 'en'::text
           WHEN lower(l.language::text) = 'cym'::text THEN 'cy'::text
           ELSE NULL::text
           END                                                                                             AS language,
       b.blpu_state                                                                                        AS blpustate,
       b.logical_status                                                                                    AS logicalstatus,
       d.post_town                                                                                         AS posttown,
       upper(regexp_replace(d.postcode::text, '[ \\t]+'::text, ' '::text))                                 AS postcode,
       concat(b.latitude, ',', b.longitude)                                                                AS location,
       d.po_box_number                                                                                     AS poboxnumber,
       asd.administrative_area                                                                             AS localAuthority,
       to_tsvector('english'::regconfig, ((COALESCE(array_to_string(
                                                            ARRAY [btrim(d.sub_building_name::text), btrim(d.building_name::text)],
                                                            ', '::text), ''::text) || COALESCE(array_to_string(
                                                                                                       ARRAY [btrim(''::text || d.building_number), btrim(d.dependent_thoroughfare::text), btrim(d.thoroughfare::text)],
                                                                                                       ' '::text),
                                                                                               ''::text)) || COALESCE(
                                                  array_to_string(
                                                          ARRAY [btrim(d.double_dependent_locality::text), btrim(d.dependent_locality::text)],
                                                          ' '::text), ''::text)) || COALESCE(
                                                 CASE
                                                     WHEN b.country::text = 'S'::text THEN 'GB-SCT'::text
                                                     WHEN b.country::text = 'E'::text THEN 'GB-ENG'::text
                                                     WHEN b.country::text = 'W'::text THEN 'GB-WLS'::text
                                                     WHEN b.country::text = 'N'::text THEN 'GB-NIR'::text
                                                     ELSE NULL::text
                                                     END,
                                                 ''::text))                                                AS address_lookup_ft_col
FROM __schema__.abp_delivery_point d
         JOIN __schema__.abp_blpu b ON b.uprn = d.uprn
         JOIN __schema__.abp_lpi l ON l.uprn = b.uprn
         JOIN __schema__.abp_street_descriptor asd on l.usrn = asd.usrn;

CREATE INDEX IF NOT EXISTS __schema__.address_lookup_ft_col_idx
    ON __schema__.address_lookup USING gin (address_lookup_ft_col);

CREATE INDEX IF NOT EXISTS __schema__.address_lookup_postcode_idx
    ON __schema__.address_lookup (postcode);

SELECT 'done' as "status" INTO __schema__.address_lookup_view_created;
