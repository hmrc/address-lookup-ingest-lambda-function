CREATE OR REPLACE FUNCTION create_address_lookup_combined_view(the_schema_name VARCHAR)
    RETURNS BOOLEAN
    LANGUAGE plpgsql
AS
$$
BEGIN
    EXECUTE format('SET search_path = %s', the_schema_name);

    DROP MATERIALIZED VIEW IF EXISTS address_lookup_combined CASCADE;

    CREATE OR REPLACE VIEW address_lookup_geographic_and_local_authority AS
    WITH base AS (SELECT l.uprn                                                                            AS uprn,
                         b.parent_uprn                                                                     AS parent_uprn,
                         l.usrn                                                                            AS usrn,
                         CASE WHEN o.organisation != '' THEN o.organisation ELSE '' END                    AS organisation_name,
                         CASE WHEN l.sao_text != '' THEN l.sao_text ELSE '' END
                             || CASE
                                    WHEN l.sao_start_number IS NOT NULL AND l.sao_start_suffix = '' AND
                                         l.sao_end_number IS NULL
                                        THEN l.sao_start_number::varchar(4) || ' '
                                    WHEN l.sao_start_number IS NULL THEN ''
                                    ELSE l.sao_start_number::varchar(4) || ''
                             END
                             || CASE
                                    WHEN l.sao_start_suffix != '' AND l.sao_end_number IS NULL
                                        THEN l.sao_start_suffix || ', '
                                    WHEN l.sao_start_suffix != '' AND l.sao_end_number IS NOT NULL
                                        THEN l.sao_start_suffix
                                    ELSE ''
                             END
                             || CASE
                                    WHEN l.sao_end_suffix != '' AND l.sao_end_number IS NOT NULL THEN '-'
                                    WHEN l.sao_start_number IS NOT NULL AND l.sao_end_number IS NOT NULL THEN '-'
                                    ELSE ''
                             END
                             || CASE
                                    WHEN l.sao_end_number IS NOT NULL AND l.sao_end_suffix = ''
                                        THEN l.sao_end_number::varchar(4) || ' '
                                    WHEN l.sao_end_number IS NULL THEN ''
                                    ELSE l.sao_end_number::varchar(4)
                             END
                             || CASE WHEN l.sao_end_suffix != '' THEN l.sao_end_suffix ELSE '' END         AS line1,
                         CASE WHEN l.pao_text != '' THEN l.pao_text ELSE '' END
                             || CASE
                                    WHEN l.pao_start_number IS NOT NULL AND l.pao_start_suffix = '' AND
                                         l.pao_end_number IS NULL
                                        THEN l.pao_start_number::varchar(4) || ' '
                                    WHEN l.pao_start_number IS NULL THEN ''
                                    ELSE l.pao_start_number::varchar(4) || ''
                             END
                             || CASE
                                    WHEN l.pao_start_suffix != '' AND l.pao_end_number IS NULL
                                        THEN l.pao_start_suffix || ', '
                                    WHEN l.pao_start_suffix != '' AND l.pao_end_number IS NOT NULL
                                        THEN l.pao_start_suffix
                                    ELSE ''
                             END
                             || CASE
                                    WHEN l.pao_end_suffix != '' AND l.pao_end_number IS NOT NULL THEN '-'
                                    WHEN l.pao_start_number IS NOT NULL AND l.pao_end_number IS NOT NULL THEN '-'
                                    ELSE ''
                             END
                             || CASE
                                    WHEN l.pao_end_number IS NOT NULL AND l.pao_end_suffix = ''
                                        THEN l.pao_end_number::varchar(4) || ' '
                                    WHEN l.pao_end_number IS NULL THEN ''
                                    ELSE l.pao_end_number::varchar(4)
                             END
                             || CASE WHEN l.pao_end_suffix != '' THEN l.pao_end_suffix ELSE '' END
                             || CASE WHEN s.street_description != '' THEN s.street_description ELSE '' END AS line2,
                         CASE WHEN s.locality != '' THEN s.locality ELSE '' END                            AS line3,
                         CASE WHEN s.town_name != '' THEN s.town_name ELSE '' END                          AS town,
                         UPPER(CASE
                                   WHEN b.country::text = 'S'::text THEN 'GB'::text
                                   WHEN b.country::text = 'E'::text THEN 'GB'::text
                                   WHEN b.country::text = 'W'::text THEN 'GB'::text
                                   WHEN b.country::text = 'N'::text THEN 'GB'::text
                                   WHEN b.country::text = 'L'::text AND substring(b.postcode_locator::text, 1, 1) = 'G'::text
                                       THEN 'GG'::text
                                   WHEN b.country::text = 'L'::text AND substring(b.postcode_locator::text, 1, 1) = 'J'::text
                                       THEN 'JE'::text
                                   WHEN b.country::text = 'M'::text THEN 'IM'::text
                                   ELSE 'NOT_FOUND'::text
                             END)                                                                          AS country_code,
                         b.local_custodian_code                                                            AS local_custodian_code,
                         UPPER(CASE
                                   WHEN lower(l.language::text) = 'eng'::text THEN 'en'::text
                                   WHEN lower(l.language::text) = 'cym'::text THEN 'cy'::text
                                   ELSE NULL::text
                             END)                                                                          AS language,
                         CASE WHEN b.postcode_locator != '' THEN b.postcode_locator ELSE '' END            AS post_code,
                         UPPER(
                                 CASE
                                     WHEN lower(l.language::text) = 'eng'::text THEN 'en'::text
                                     WHEN lower(l.language::text) = 'cym'::text THEN 'cy'::text
                                     ELSE NULL::text
                                     END
                         )                                                                                 AS language,
                         b.blpu_state                                                                      AS blpu_state,
                         b.logical_status                                                                  AS logical_status,
                         concat(b.latitude, ',', b.longitude)                                              AS location
                  FROM abp_blpu AS b
                           INNER JOIN abp_lpi AS l ON b.uprn = l.uprn
                           INNER JOIN abp_street_descriptor AS s ON l.usrn = s.usrn
                           FULL OUTER JOIN abp_organisation AS o
                                           ON (l.uprn = o.uprn AND l.language = s.language)
                  WHERE l.language = 'ENG')
    SELECT uprn,
           parent_uprn,
           usrn,
           organisation_name,
           line1,
           line2,
           line3,
           town,
           country_code,
           local_custodian_code,
           post_code,
           to_tsvector('english'::regconfig,
                       array_to_string(
                               ARRAY [
                                   NULLIF(btrim(organisation_name)::text, ''),
                                   NULLIF(btrim(line1)::text, ''),
                                   NULLIF(btrim(line2)::text, ''),
                                   NULLIF(btrim(line3)::text, ''),
                                   NULLIF(btrim(town)::text, ''),
                                   NULLIF(btrim(post_code)::text, '')],
                               ' '::text)
           ) AS address_lookup_geographic_and_local_authority_ft_col
    FROM base;

    insert into __schema__.tmp_tbl(result, messages)
    values ('true', 'create gla view');

    CREATE MATERIALIZED VIEW address_lookup_combined AS
    WITH local_authority_addresses AS (SELECT al2.uprn
                                       FROM address_lookup_geographic_and_local_authority al2
                                       EXCEPT
                                       SELECT al.uprn
                                       FROM address_lookup al)
    SELECT al2.uprn,
           organisation_name,
           line1,
           line2,
           line3,
           town,
           post_code,
           country_code,
           address_lookup_geographic_and_local_authority_ft_col AS address_lookup_combined_ft_col
    FROM address_lookup_geographic_and_local_authority al2
             JOIN local_authority_addresses AS g ON g.uprn = al2.uprn
    UNION
    SELECT uprn,
           organisation_name,
           line1,
           line2,
           line3,
           posttown,
           postcode,
           country_code,
           address_lookup_ft_col
    FROM address_lookup;

    insert into __schema__.tmp_tbl(result, messages)
    values (true, 'create combined view');

    CREATE INDEX IF NOT EXISTS address_lookup_combined_ft_col_idx
        ON address_lookup_combined USING gin (address_lookup_combined_ft_col);
    insert into __schema__.tmp_tbl(result, messages)
    values (true, 'create combined ft index');

    CREATE INDEX IF NOT EXISTS address_lookup_combined_uprn_idx
        ON address_lookup_combined (uprn);
    insert into __schema__.tmp_tbl(result, messages)
    values (true, 'create combined uprn index');

    CREATE INDEX IF NOT EXISTS address_lookup_combined_postcode_idx
        ON address_lookup_combined (post_code);
    insert into __schema__.tmp_tbl(result, messages)
    values (true, 'create combined postcode index');

    CREATE OR REPLACE VIEW public.address_lookup_combined_view AS
    SELECT *
    FROM address_lookup_combined;

    RETURN TRUE;
END;
$$
