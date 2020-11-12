from os import listdir, chdir, path, open, read
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
        return 1

    epochs_available = [int(x) for x in listdir(base_dir)]
    epochs_available.sort(reverse=True)
    if len(epochs_available) > 0:
        latest_epoch = epochs_available[0]
        epoch_base_dir = base_dir + "/{}".format(latest_epoch)
        return (latest_epoch, epoch_base_dir)
    else:
        print("No epochs available to process - finishing.")
        return None


def process_files(base_dir):
    epoch_base_dir = base_epoch_dir(base_dir)
    if epoch_base_dir is None:
        return 2

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

    (epoch, epoch_base_dir) = base_epoch_dir(base_dir)
    if epoch_base_dir is None:
        return 2
    chdir(epoch_base_dir)

    print("Ingesting from ...{}".format(epoch_base_dir))

    password = getSecret('address_lookup_rds_password', context={'role': 'address_lookup_file_download'})

    # Make sure that the schema exists otherwise this will fall over
    with psycopg2.connect(
            host="addresslookup.cobnrd9qoh0u.eu-west-2.rds.amazonaws.com",
            port=5432,
            database="postgres",
            schema=epoch,
            user="root",
            password=password) as conn:
        cur = conn.cursor()
        cur.execute('SELECT version()')
        db_version = cur.fetchone()
        print("DB Version retrieved as {}".format(db_version))

        create_db_schema(conn, cur)
        ingest_data(cur)

        cur.close()
    conn.close()


def create_db_schema(db_conn, db_cur):
    with open('create_db_schema.sql', "r") as f:
        sql = f.read()
    db_cur.execute(sql)
    # db_conn.commit()


def ingest_data(db_cur):
    insert_data_into_table(db_cur, 'abp_blpu', 'ID21_BLPU_Records.csv')
    insert_data_into_table(db_cur, 'abp_delivery_point', 'ID28_DPA_Records.csv')
    insert_data_into_table(db_cur, 'abp_lpi', 'ID24_LPI_Records.csv')
    insert_data_into_table(db_cur, 'abp_crossref', 'ID23_XREF_Records.csv')
    insert_data_into_table(db_cur, 'abp_classification', 'ID32_Class_Records.csv')
    insert_data_into_table(db_cur, 'abp_street', 'ID11_Street_Records.csv')
    insert_data_into_table(db_cur, 'abp_street_descriptor', 'ID15_StreetDesc_Records.csv')
    insert_data_into_table(db_cur, 'abp_organisation', 'ID31_Org_Records.csv')
    insert_data_into_table(db_cur, 'abp_successor', 'ID30_Successor_Records.csv')


def insert_data_into_table(db_cur, table, file):
    db_cur.copy_expert(table + " FROM STDIN DELIMITER ',' CSV HEADER", file)


if __name__ == "__main__":
    ingest_handler(None, None)
