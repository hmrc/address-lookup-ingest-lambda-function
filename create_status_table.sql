CREATE TABLE IF NOT EXISTS public.address_lookup_status (
    schema_name VARCHAR(64) NOT NULL PRIMARY KEY,
    status      VARCHAR(32) NOT NULL,
    timestamp   TIMESTAMP NOT NULL
);

ALTER TABLE public.address_lookup_status
ADD COLUMN error_message VARCHAR NULL;
