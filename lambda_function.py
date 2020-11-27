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
    with initial_connection() as epoch_schema_con:
        with epoch_schema_con.cursor() as cur:
            cur.execute("""
                CREATE USER addresslookupingester;
                GRANT rds_iam TO addresslookupingester;
            """)


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
            cur.execute("""SELECT status FROM public.address_lookup_status WHERE schema_name = %s""",
                        (db_schema_name,))
            status = cur.fetchone()[0]  # If not rows found then error will be raised

    epoch_schema_con.close()

    return status


def cleanup_handler(epoch, context):
    """
    Handler for cleaning up the filesystem
    :param epoch:
    """
    cleanup_old_epoch_directories(epoch)
    cleanup_processed_csvs(epoch)


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
    lookup_view_sql = read_db_lookup_view_and_indexes_sql(db_schema_name)

    epoch_schema_con = async_epoch_schema_connection(db_schema_name)

    try:
        with epoch_schema_con.cursor() as cur:
            cur.execute(lookup_view_sql)
    except Exception, e:
        print('There was a warning.  This is the info we have about it: %s' % e)


def ensure_status_table():
    with default_connection() as default_con:
        with default_con.cursor() as cur:
            print("Creating status table if it does not exist...")
            lookup_view_sql = open('create_status_table.sql', 'r').read()
            cur.execute(lookup_view_sql)

            def drop_schema(schema_to_drop):
                print("Dropping old schema {}".format(schema_to_drop))
                sql_to_execute = """DROP SCHEMA IF EXISTS {} CASCADE; 
                    DELETE FROM public.address_lookup_status WHERE schema_name = '{}';"""
                cur.execute(sql.SQL(sql_to_execute.format(schema_to_drop, schema_to_drop)))

            schemas_to_drop = get_schemas_to_drop(cur)
            map(drop_schema, schemas_to_drop)

    default_con.close()
    return schemas_to_drop


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
            cur.execute("INSERT INTO public.address_lookup_status VALUES(%s, 'schema_created', now());""",
                        (db_schema_name,))

    epoch_schema_con.close()


def initial_connection():
    return create_init_connection()


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
    conn = psycopg2.connect(host=con_params['host'], port=con_params['port'], database=con_params['database'],
                            user=con_params['user'],
                            password=con_params['password'], async=1, options=con_params['options'])
    wait(conn)
    return conn


def create_init_connection():
    con_params = db_con_params('')
    return psycopg2.connect(
        host=con_params['host'],
        port=con_params['port'],
        database=con_params['database'],
        user=con_params['user'],
        password=getSecret('address_lookup_rds_password', context=credstash_context),
    )


def create_connection(options):
    con_params = db_con_params(options)
    return psycopg2.connect(
        host=con_params['host'],
        port=con_params['port'],
        database=con_params['database'],
        user=con_params['user'],
        password=con_params['password'],
        options=con_params['options'],
    )


def db_con_params(options):
    client = boto3.client('rds')
    db_host = getSecret('address_lookup_rds_host', context=credstash_context)
    db_name = getSecret('address_lookup_rds_database', context=credstash_context)
    db_user = getSecret('address_lookup_rds_user', context=credstash_context)
    token = client.generate_db_auth_token(DBHostname=db_host, Port=5432, DBUsername=db_user, Region='eu-west-2')

    return {
        "host"    : db_host,
        "port"    : 5432,
        "database": db_name,
        "user"    : db_user,
        "password": token,
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
    print(create_schema('79'))
