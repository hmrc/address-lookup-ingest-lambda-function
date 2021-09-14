SELECT EXISTS(
   SELECT 1
   FROM pg_matviews
   WHERE schemaname = __schema__
     AND matviewname = 'address_lookup'
)