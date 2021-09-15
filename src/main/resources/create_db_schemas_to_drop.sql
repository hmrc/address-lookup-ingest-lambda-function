SELECT  schema_name
FROM    public.address_lookup_status
WHERE   schema_name NOT IN (
    SELECT      schema_name
    FROM        public.address_lookup_status
    WHERE       status = 'finalised'
    ORDER BY    timestamp DESC
    LIMIT 1
)