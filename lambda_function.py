from os import listdir, chdir, path
from time import time
from split_abp_files import createCSV
import psycopg2
from credstash import getSecret

def process_handler(event, context):
    root_dir = "/mnt/efs"
    process_files(root_dir + "/abp")
    process_files(root_dir + "/abi")


def process_files(baseDir):
    if not path.exists(baseDir):
        print("{} directory does not exist - finishing.".format(baseDir))
        return 1

    epochs_available = [int(x) for x in listdir(baseDir)]
    epochs_available.sort(reverse=True)
    if len(epochs_available) > 0:
        latest_epoch = epochs_available[0]
    else:
        print("No epochs available to process - finishing.")
        return 2

    epoch_base_dir = baseDir + "/{}".format(latest_epoch)
    print("Processing files in dir: {}, for epoch: {}".format(epoch_base_dir, latest_epoch))

    print(listdir(epoch_base_dir))

    chdir(epoch_base_dir)
    start_time = time()
    createCSV(epoch_base_dir)
    end_time = time()

    print("Time taken to split files: ", end_time - start_time)

    return 0


def ingest_handler(event, context):
    print("Ingesting ...")

    password = getSecret('address_lookup_rds_password',context={'role': 'address_lookup_file_download'})

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="postgres",
        user="root",
        password=password)

    cur = conn.cursor()
    cur.execute('SELECT version()')
    db_version = cur.fetchone()
    cur.close()

    print("DB Version retrieved as {}".format(db_version))


if __name__ == "__main__":
    ingest_handler(None, None)
