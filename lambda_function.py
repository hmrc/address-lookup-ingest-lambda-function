from os import listdir, chdir, path, read
from time import time
from split_abp_files import createCSV
import psycopg2
from credstash import getSecret


def process_handler(event, context):
    root_dir = "/mnt/efs"
    process_files(root_dir + "/abp")
    process_files(root_dir + "/abi")


def base_epoch_dir(base_dir):
    if not path.exists(base_dir):
        print("{} directory does not exist - finishing.".format(base_dir))
        return None

    epochs_available = [int(x) for x in listdir(base_dir)]
    epochs_available.sort(reverse=True)
    if len(epochs_available) > 0:
        latest_epoch = epochs_available[0]
        epoch_base_dir = base_dir + "/{}".format(latest_epoch)
        return (str(latest_epoch), epoch_base_dir)
    else:
        print("No epochs available to process - finishing.")
        return None


def process_files(base_dir):
    (epoch, epoch_base_dir) = base_epoch_dir(base_dir)
    if epoch_base_dir is None:
        return 1

    print("Processing files in dir: {}".format(epoch_base_dir))

    print(listdir(epoch_base_dir))

    chdir(epoch_base_dir)
    start_time = time()
    createCSV(epoch_base_dir)
    end_time = time()

    print("Time taken to split files: ", end_time - start_time)

    return 0


def ingest_handler(event, context):
    root_dir = "/mnt/efs"
    base_dir = root_dir + "/abp"

    result = base_epoch_dir(base_dir)
    if result is None:
        return 1
    (epoch, epoch_base_dir) = result
    db_schema_name = "ab{}".format(epoch)

    schema_sql = read_db_schema_sql(db_schema_name)

    chdir(epoch_base_dir)

    print("Ingesting from ...{}".format(epoch_base_dir))


    # Make sure that the schema exists otherwise this will fall over
    with default_connection() as default_con:
        cur = default_con.cursor()
        create_db_schema(default_con, cur, db_schema_name)
        cur.close()
    default_con.close()

    with epoch_schema_connection(epoch) as epoch_schema_con:
        cur = epoch_schema_con.cursor()
        create_db_schema_objects(epoch_schema_con, cur, schema_sql)
        ingest_data(cur, db_schema_name, epoch_base_dir)
        cur.close()
    epoch_schema_con.close()


def default_connection():
    password = getSecret('address_lookup_rds_password', context={'role': 'address_lookup_file_download'})
    return psycopg2.connect(
        host="addresslookup.cobnrd9qoh0u.eu-west-2.rds.amazonaws.com",
        port=5432,
        database="postgres",
        user="root",
        password=password)


def epoch_schema_connection(epoch):
    password = getSecret('address_lookup_rds_password', context={'role': 'address_lookup_file_download'})
    return psycopg2.connect(
        host="addresslookup.cobnrd9qoh0u.eu-west-2.rds.amazonaws.com",
        port=5432,
        database="postgres",
        user="root",
        password=password,
        options='-c search_path={}'.format(epoch))


def create_db_schema(db_con, db_cur, schema_name):
    db_cur.execute("CREATE SCHEMA {}".format(schema_name))
    db_con.commit()


def read_db_schema_sql(db_schema_name):
    sql = open('create_db_schema.sql', 'r').read().replace("CREATE TABLE abp", "CREATE TABLE {}.abp".format(db_schema_name))
    return sql


def create_db_schema_objects(db_conn, db_cur, schema_sql):
    db_cur.execute(schema_sql)
    db_conn.commit()


def ingest_data(db_cur, db_schema_name, epoch_dir):
    insert_data_into_table(db_cur, db_schema_name + '.abp_blpu', epoch_dir + '/ID21_BLPU_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_delivery_point', epoch_dir + '/ID28_DPA_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_lpi', epoch_dir + '/ID24_LPI_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_crossref', epoch_dir + '/ID23_XREF_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_classification', epoch_dir + '/ID32_Class_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_street', epoch_dir + '/ID11_Street_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_street_descriptor', epoch_dir + '/ID15_StreetDesc_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_organisation', epoch_dir + '/ID31_Org_Records.csv')
    insert_data_into_table(db_cur, db_schema_name + '.abp_successor', epoch_dir + '/ID30_Successor_Records.csv')


def insert_data_into_table(db_cur, table, file):
    print("Ingesting {} into table {}".format(file, table))
    with open(file, 'r') as f:
        db_cur.copy_expert("COPY " + table + " FROM STDIN DELIMITER ',' CSV HEADER", f)


if __name__ == "__main__":
    ingest_handler(None, None)
