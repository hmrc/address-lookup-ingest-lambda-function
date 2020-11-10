import os


def handler(event, context):
    print("Listing files\n")

    file_list = os.listdir("/mnt/efs")

    return file_list


if __name__ == "__main__":
    handler(None, None)
