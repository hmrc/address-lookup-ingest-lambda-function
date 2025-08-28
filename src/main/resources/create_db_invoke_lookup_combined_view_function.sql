BEGIN TRANSACTION;
drop table if exists __schema__.tmp_tbl;
create table __schema__.tmp_tbl(
    result boolean,
    messages text
);

insert into __schema__.tmp_tbl(result, messages)
values (create_address_lookup_combined_view('__schema__'), 'success');
COMMIT;
