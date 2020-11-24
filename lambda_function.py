from os import listdir, chdir, path
from time import time, gmtime, strftime

import select

from split_abp_files import createCSV
import psycopg2
import psycopg2.extensions
from credstash import getSecret


# batch_path is the full path to the batch directory, eg <root>/abp/79/2
def process_handler(batch_info, context):
    batch_dir = batch_info['batchDir']

    process_files(batch_dir)


def create_schema_handler(epoch, context):
    return create_schema(epoch)


def ingest_handler(batch_info, context):
    db_schema_name = batch_info['schemaName']
    batch_dir = batch_info['batchDir']

    ingest_files(db_schema_name, batch_dir)


def create_lookup_view_and_indexes_handler(db_schema_name, context):
    create_lookup_view_and_indexes(db_schema_name)


# noinspection SqlResolve
def check_status_handler(db_schema_name, context):
    with epoch_schema_connection(db_schema_name) as epoch_schema_con:
        with epoch_schema_con.cursor() as cur:
            cur.execute("""SELECT status FROM public.address_lookup_status WHERE schema_name = %s""",
                        (db_schema_name,))
            status = cur.fetchone()[0] #If not rows found then error will be raised

    epoch_schema_con.commit()
    epoch_schema_con.close()

    return status


def base_epoch_dir(base_dir):
    if not path.exists(base_dir):
        print("{} directory does not exist - finishing.".format(base_dir))
        return None

    epochs_available = [int(x) for x in listdir(base_dir)]
    epochs_available.sort(reverse=True)
    if len(epochs_available) > 0:
        latest_epoch = epochs_available[0]
        epoch_base_dir = base_dir + "/{}".format(latest_epoch)
        return str(latest_epoch), epoch_base_dir
    else:
        print("No epochs available to process - finishing.")
        return None


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

    init_schema(db_schema_name)
    schema_sql = read_db_schema_sql(db_schema_name)
    populate_schema(db_schema_name, schema_sql)

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
        print 'There was a warning.  This is the info we have about it: %s' %(e)


def init_schema(db_schema_name):
    print("Creating schema {}".format(db_schema_name))
    with default_connection() as default_con:
        with default_con.cursor() as cur:
            create_db_schema(default_con, cur, db_schema_name)

    default_con.close()


def populate_schema(db_schema_name, schema_sql):
    print("Populating schema with tables {}".format(db_schema_name))
    with epoch_schema_connection(db_schema_name) as epoch_schema_con:
        with epoch_schema_con.cursor() as cur:
            create_db_schema_objects(epoch_schema_con, cur, schema_sql)

    epoch_schema_con.close()


def default_connection():
    return create_connection('')


def epoch_schema_connection(epoch):
    return create_connection('-c search_path={}'.format(epoch))


def async_epoch_schema_connection(epoch):
    return create_async_connection('-c search_path={}'.format(epoch))


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
    con_params = db_con_params(options,
                               getSecret('address_lookup_rds_password',
                                                  context={'role': 'address_lookup_file_download'}),
                               getSecret('address_lookup_db_host',
                                         context={'role': 'address_lookup_file_download'})
                               )
    conn = psycopg2.connect(host=con_params['host'], port=con_params['port'], database=con_params['database'], user=con_params['user'],
                               password=con_params['password'], async=1, options=con_params['options'])
    wait(conn)
    return conn


def create_connection(options):
    con_params = db_con_params(options,
                               getSecret('address_lookup_rds_password',
                                                  context={'role': 'address_lookup_file_download'}),
                               getSecret('address_lookup_db_host',
                                         context={'role': 'address_lookup_file_download'})
                               )
    return psycopg2.connect(
        host=con_params['host'],
        port=con_params['port'],
        database=con_params['database'],
        user=con_params['user'],
        password=con_params['password'],
        options=con_params['options'],
    )


def db_con_params(options, password, host):
    return {
        "host": host,
        "port": 5432,
        "database": "addressbasepremium",
        "user": "root",
        "password": password,
        "options": options
    }


def create_db_schema(db_con, db_cur, schema_name):
    db_cur.execute("CREATE SCHEMA IF NOT EXISTS {}".format(schema_name))
    db_con.commit()


# This is a little hacky - need to think about a better way to inject the schema name into the schema file.
def read_db_schema_sql(db_schema_name):
    sql = open('create_db_schema.sql', 'r').read().replace("__schema__", db_schema_name)
    return sql


def read_db_indexes_sql(db_schema_name):
    sql = open('create_db_schema_indexes.sql', 'r').read().replace("__schema__", db_schema_name)
    return sql


def read_db_lookup_view_and_indexes_sql(db_schema_name):
    sql = open('create_db_lookup_view_and_indexes.sql', 'r').read().replace("__schema__", db_schema_name)
    return sql


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
    print(check_status_handler("ab79_20201120_161341", None))
