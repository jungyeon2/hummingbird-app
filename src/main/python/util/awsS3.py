import os
import boto


def make_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)
    return


def s3_download(source_key, target_file, bucket, session):
    s3 = session.resource(service_name='s3', verify=False)
    s3.meta.client.download_file(bucket, source_key, target_file)
    return


def s3_upload(source_file, s3_path, bucket, session):
    s3 = session.resource(service_name='s3', verify=False)
    bucket = s3.Bucket(bucket)
    bucket.upload_file(source_file, s3_path)


def s3_get_file_names(access_key, secret_key, bucket_name, path):
    conn = boto.connect_s3(access_key, secret_key)
    bucket = conn.get_bucket(bucket_name, validate=False)
    files = bucket.list(path, delimiter="/")

    file_list = []

    for f in files:
        if f is not None:
            file_name_only = f.name[len(path):]
            file_list.append(file_name_only)

    return file_list


def get_missing_items(list1, list2):
    missing_items = []

    for i in list1:
        if i not in list2:
            missing_items.append(i)

    return missing_items
