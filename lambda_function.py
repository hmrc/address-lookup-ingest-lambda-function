from os import chdir, path, walk, remove
from time import time, gmtime, strftime
from shutil import rmtree
import boto3

import psycopg2
import psycopg2.extensions
import select
from credstash import getSecret
from psycopg2 import sql

from split_abp_files import createCSV

root_dir = '/mnt/efs/'
credstash_context = {'role': 'address_lookup_file_download'}


def dbuser_init_handler(nothing, context):
    with initial_connection_connection() as epoch_schema_con:
        with epoch_schema_con.cursor() as cur:
            dbuser_init(cur)


def process_handler(batch_info, context):
    """
    Collects same types of data into same files across all downloaded zip files.
    :param batch_info: json structure that holds path to directory containing files to process, eg <root>/abp/79/2
    """

    batch_dir = batch_info['batchDir']
    process_files(batch_dir)


def create_schema_handler(epoch, context):
    """
    Creates a new schema for ingestion of new data.
    :param epoch: the 'version' of data, eg 79
    """

    return create_schema(epoch)


def ingest_handler(batch_info, context):
    """
    Imports processed data into database using the COPY instruction.
    :param batch_info: json structure that holds name of schema for data and the directory from which to import data
    """

    db_schema_name = batch_info['schemaName']
    batch_dir = batch_info['batchDir']

    ingest_files(db_schema_name, batch_dir)


def create_lookup_view_and_indexes_handler(db_schema_name, context):
    """
    Creates view that brings tables together to make querying easier for address lookup purposes.
    :param db_schema_name: schema where tables reside and where the view will be created.
    """

    create_lookup_view_and_indexes(db_schema_name)


# noinspection SqlResolve
def check_status_handler(db_schema_name, context):
    """
    Checks whether the lookup view has been created
    :param db_schema_name: schema where view is to be created
    """

    with epoch_schema_connection(db_schema_name) as epoch_schema_con:
        with epoch_schema_con.cursor() as cur:
            cur.execute("""SELECT status, error_message FROM public.address_lookup_status WHERE 
            schema_name = 
            %s""",
                        (db_schema_name,))
            status, error_message = cur.fetchone()  # If not rows found then error will be raised

    epoch_schema_con.close()

    return {'status': status, 'errorMessage': error_message}


def finalise_handler(epoch_data, context):
    epoch = epoch_data['epoch']
    schema_name = epoch_data['schemaName']

    schema_ok = is_new_schema_within_change_tolerance(schema_name)
    if schema_ok:
        switch_address_lookup_view_to_new(schema_name)
        cleanup_old_epoch_directories(epoch)
        # cleanup_processed_csvs(epoch)

    return schema_ok


def switch_address_lookup_view_to_new(schema_name):
    with epoch_schema_connection(schema_name) as epoch_schema_con:
        with epoch_schema_con.cursor() as cur:
            cur.execute("""
                CREATE OR REPLACE VIEW public.address_lookup AS SELECT * FROM address_lookup;
    
                GRANT SELECT ON public.address_lookup TO addresslookupreader;
                """)
    epoch_schema_con.close()


def dbuser_init(db_cur):
    db_user = getSecret('address_lookup_rds_ingest_user', context=credstash_context)
    db_name = getSecret('address_lookup_rds_database', context=credstash_context)
    db_cur.execute("SELECT * FROM pg_user WHERE usename = %s", (db_user,))
    existing_user = db_cur.fetchone()
    if existing_user is None:
        print("Creating application user")
        db_cur.execute(sql.SQL("""
            REVOKE CREATE ON SCHEMA public FROM PUBLIC;
            CREATE USER {};
            GRANT rds_iam TO {};
            GRANT ALL ON DATABASE {} to {};
            GRANT CREATE ON SCHEMA public TO {};
        """.format(db_user, db_user, db_name, db_user, db_user)))

    db_ro_user = getSecret('address_lookup_rds_readonly_user', context=credstash_context)
    db_ro_password = getSecret('address_lookup_rds_readonly_password', context=credstash_context)
    db_cur.execute("SELECT * FROM pg_user WHERE usename = %s", (db_ro_user,))
    existing_user = db_cur.fetchone()
    if existing_user is None:
        print("Creating application user")
        db_cur.execute(sql.SQL("""
            CREATE USER {} ENCRYPTED PASSWORD '{}';
            GRANT CONNECT ON DATABASE {} TO {};
        """.format(db_ro_user, db_ro_password, db_name, db_ro_user, db_ro_user)))


# This will get called with the batch directory
def process_files(batch_dir):
    print("Processing files in dir: {}".format(batch_dir))

    chdir(batch_dir)
    start_time = time()
    createCSV(batch_dir)
    end_time = time()

    print("Time taken to split files: ", end_time - start_time)

    return batch_dir


def create_schema(epoch):
    db_schema_name = strftime("ab{}".format(epoch) + "_%Y%m%d_%H%M%S", gmtime())
    print("Using schema name {}".format(db_schema_name))

    ensure_status_table()
    drop_old_schemas()
    init_schema(db_schema_name)
    schema_sql = read_db_schema_sql(db_schema_name)
    create_schema_objects(db_schema_name, schema_sql)

    return db_schema_name


# batch_dir will be of form <path>/ab[p|i]/<epoch>/<batch>
def ingest_files(db_schema_name, batch_dir):
    print("Ingesting from {} to {}".format(batch_dir, db_schema_name))

    with epoch_schema_connection(db_schema_name) as epoch_schema_con:
        with epoch_schema_con.cursor() as cur:
            ingest_data(cur, db_schema_name, batch_dir)

    epoch_schema_con.commit()
    epoch_schema_con.close()


def create_lookup_view_and_indexes(db_schema_name):
    print("Creating lookup_view {}".format(db_schema_name))
    lookup_view_sp_sql = read_db_lookup_view_and_indexes_sql(db_schema_name)

    with epoch_schema_connection(db_schema_name) as epoch_schema_con:
        with epoch_schema_con.cursor() as cur:
            cur.execute(lookup_view_sp_sql)

    epoch_schema_con_async = async_epoch_schema_connection(db_schema_name)
    try:
        with epoch_schema_con_async.cursor() as cur:
            sql_to_execute = """BEGIN TRANSACTION;
            CALL create_address_lookup_view('{}');
            COMMIT;"""

            cur.execute(sql_to_execute.format(db_schema_name))
    except Exception, e:
        print('There was a warning.  This is the info we have about it: %s' % e)


def ensure_status_table():
    with default_connection() as default_con:
        with default_con.cursor() as cur:
            print("Creating status table if it does not exist...")
            lookup_view_sql = open('create_status_table.sql', 'r').read()
            cur.execute(lookup_view_sql)

    default_con.close()


def drop_old_schemas():
    with default_connection() as default_con:
        with default_con.cursor() as cur:
            print("Dropping old schemas...")
            def drop_schema(schema_to_drop):
                print("Dropping old schema {}".format(schema_to_drop))
                sql_to_execute = """DROP SCHEMA IF EXISTS {} CASCADE; 
                    DELETE FROM public.address_lookup_status WHERE schema_name = '{}';"""
                cur.execute(sql.SQL(sql_to_execute.format(schema_to_drop, schema_to_drop)))

            map(drop_schema, get_schemas_to_drop(cur))

    default_con.close()


def is_new_schema_within_change_tolerance(latest_schema_name):
    with default_connection() as default_con:
        with default_con.cursor() as cur:
            print("Checking new schema...")
            previous_schema = get_schema_to_compare(cur)
            sql_to_execute = """SELECT COUNT(*) FROM {}.abp_street_descriptor"""
            previous_count = cur.execute(sql.SQL(sql_to_execute.format(previous_schema)))[0]
            latest_count = cur.execute(sql.SQL(sql_to_execute.format(latest_schema_name)))[0]
    default_con.close()

    percentage_change = ((latest_count - previous_count) / previous_count) * 100.0
    return percentage_change < 0.3 and percentage_change > 0


def cleanup_old_epoch_directories(latest_epoch):
    """
    Cleans all epoch dirs that are not latest_epoch
    :param latest_epoch:
    """
    (dirname, dir_names, filenames) = walk(root_dir).next()
    for d in dir_names:
        if d != latest_epoch:
            rmtree(path.join(dirname, d))


# <root>/<product>/<epoch>/<batch_dir>
def cleanup_processed_csvs(epoch):
    """
    Deletes all *.csv files anywhere in the tree
    :param epoch: the latest epoch
    """
    epoch_dir = root_dir + epoch

    for (dirname, dir_names, filenames) in walk(epoch_dir):
        for f in filenames:
            if f.endswith('.csv') or f == 'processing.done':
                remove(path.join(dirname, f))


# noinspection SqlResolve
def get_schemas_to_drop(db_cur):
    db_cur.execute(
        """SELECT schema_name
           FROM public.address_lookup_status
           WHERE schema_name NOT IN (
               SELECT schema_name
               FROM public.address_lookup_status
               WHERE status = 'completed'
               ORDER BY timestamp DESC
               LIMIT 1
           );""")

    schemas_to_drop = db_cur.fetchall()
    return map(lambda st: st[0], schemas_to_drop)


# noinspection SqlResolve
def get_schema_to_compare(db_cur):
    db_cur.execute(
        """    SELECT schema_name
               FROM public.address_lookup_status
               WHERE status = 'completed'
               ORDER BY timestamp DESC
               LIMIT 1
           """)

    schema = db_cur.fetchone()
    return schema[0]


def init_schema(db_schema_name):
    print("Creating schema {}".format(db_schema_name))
    with default_connection() as default_con:
        with default_con.cursor() as cur:
            create_db_schema(default_con, cur, db_schema_name)

    default_con.close()


def create_schema_objects(db_schema_name, schema_sql):
    print("Populating schema with tables {}".format(db_schema_name))
    with epoch_schema_connection(db_schema_name) as epoch_schema_con:
        with epoch_schema_con.cursor() as cur:
            create_db_schema_objects(epoch_schema_con, cur, schema_sql)
            cur.execute("INSERT INTO public.address_lookup_status(schema_name, status, timestamp) VALUES(%s, 'schema_created', now());""",
                        (db_schema_name,))

    epoch_schema_con.close()


def initial_connection():
    return initial_connection_connection()


def default_connection():
    return create_connection('')


def epoch_schema_connection(db_schema_name):
    return create_connection('-c search_path={}'.format(db_schema_name))


def async_epoch_schema_connection(db_schema_name):
    return create_async_connection('-c search_path={}'.format(db_schema_name))


def wait(conn):
    while True:
        state = conn.poll()
        if state == psycopg2.extensions.POLL_OK:
            break
        elif state == psycopg2.extensions.POLL_WRITE:
            select.select([], [conn.fileno()], [])
        elif state == psycopg2.extensions.POLL_READ:
            select.select([conn.fileno()], [], [])
        else:
            raise psycopg2.OperationalError("poll() returned %s" % state)


def create_async_connection(options):
    con_params = db_con_params(options)
    conn = psycopg2.connect(
        host=con_params['host'],
        port=con_params['port'],
        database=con_params['database'],
        user=con_params['user'],
        password=con_params['password'],
        async=1,
        sslmode=con_params['sslmode'],
        sslrootcert=con_params['sslrootcert'],
        options=con_params['options'])
    wait(conn)
    return conn


def initial_connection_connection():
    db_host = getSecret('address_lookup_rds_host', context=credstash_context)
    db_name = getSecret('address_lookup_rds_database', context=credstash_context)
    db_admin_user = getSecret('address_lookup_rds_admin_user', context=credstash_context)
    db_admin_password = getSecret('address_lookup_rds_admin_password', context=credstash_context)

    return psycopg2.connect(
        host=db_host,
        port=5432,
        database=db_name,
        user=db_admin_user,
        password=db_admin_password,
        sslmode="prefer",
        sslrootcert="rds-combined-ca-bundle.pem"
    )


def create_connection(options):
    con_params = db_con_params(options)
    return psycopg2.connect(
        host=con_params['host'],
        port=con_params['port'],
        database=con_params['database'],
        user=con_params['user'],
        password=con_params['password'],
        sslmode=con_params['sslmode'],
        sslrootcert=con_params['sslrootcert'],
        options=con_params['options'],
    )


def db_con_params(options):
    client = boto3.client('rds')
    db_host = getSecret('address_lookup_rds_host', context=credstash_context)
    db_name = getSecret('address_lookup_rds_database', context=credstash_context)
    db_user = getSecret('address_lookup_rds_ingest_user', context=credstash_context)
    token = client.generate_db_auth_token(DBHostname=db_host, Port=5432, DBUsername=db_user, Region='eu-west-2')

    return {
        "host"    : db_host,
        "port"    : 5432,
        "database": db_name,
        "user"    : db_user,
        "password": token,
        "sslmode": "prefer",
        "sslrootcert": "rds-combined-ca-bundle.pem",
        "options" : options
    }


def create_db_schema(db_con, db_cur, schema_name):
    db_cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(schema_name))
    db_con.commit()


# This is a little hacky - need to think about a better way to inject the schema name into the schema file.
def read_db_schema_sql(db_schema_name):
    sqlString = open('create_db_schema.sql', 'r').read().replace("__schema__", db_schema_name)
    return sqlString


def read_db_indexes_sql(db_schema_name):
    sqlString = open('create_db_schema_indexes.sql', 'r').read().replace("__schema__", db_schema_name)
    return sqlString


def read_db_lookup_view_and_indexes_sql(db_schema_name):
    sqlString = open('create_db_lookup_view_and_indexes.sql', 'r').read().replace("__schema__", db_schema_name)
    return sqlString


def create_db_schema_objects(db_conn, db_cur, schema_sql):
    db_cur.execute(schema_sql)
    db_conn.commit()


def create_db_indexes(db_conn, db_cur, indexes_sql):
    db_cur.execute(indexes_sql)
    db_conn.commit()


def ingest_data(db_cur, db_schema_name, batch_dir):
    insert_data_into_table(db_cur, db_schema_name + '.abp_blpu', batch_dir + '/ID21_BLPU_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_delivery_point', batch_dir + '/ID28_DPA_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_lpi', batch_dir + '/ID24_LPI_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_crossref', batch_dir + '/ID23_XREF_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_classification', batch_dir + '/ID32_Class_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_street', batch_dir + '/ID11_Street_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_street_descriptor',
                           batch_dir + '/ID15_StreetDesc_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_organisation', batch_dir + '/ID31_Org_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_successor', batch_dir + '/ID30_Successor_Records.csv')


def insert_data_into_table(db_cur, table, file):
    print("Ingesting {} into table {}".format(file, table))
    with open(file, 'r') as f:
        db_cur.copy_expert("COPY " + table + " FROM STDIN DELIMITER ',' CSV HEADER", f)


if __name__ == "__main__":
    # process_handler(None, None)
    # create_lookup_view_and_indexes_handler("ab79_20201120_161341", None)
    print(create_lookup_view_and_indexes('ab80_20201204_103452'))
