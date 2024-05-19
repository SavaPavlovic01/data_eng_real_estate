import gzip
from minio import Minio
import json
import io
from datetime import datetime
import os
import dotenv

dotenv.load_dotenv()
access_key = os.environ.get("AWS_KEY")
secret_key = os.environ.get("AWS_SECRET")

def compress(data):
    compressed = gzip.compress(data.read())
    return io.BytesIO(compressed)

def decompress(data) -> bytes:
    return gzip.decompress(data)
    
def send_to_s3(data, file_name, bucket = 'test', append_date = True, compress = True):
    f = io.BytesIO(bytes(json.dumps(data), 'utf-8'))
    if compress: f = compress(f)
    client = Minio('localhost:9000', access_key=access_key, secret_key=secret_key, secure=False)
    file_name = file_name if not append_date else file_name + str(datetime.now())
    client.put_object(bucket, file_name, f , f.getbuffer().nbytes)
    return file_name

def get_from_s3(file_name, bucket) -> bytes:
    client = Minio('localhost:9000', access_key=access_key, secret_key=secret_key, secure=False)
    return client.get_object(bucket, file_name).data

import glob

def send_folder_to_s3(local_path, bucket_name, minio_path):
    assert os.path.isdir(local_path)
    client = Minio('localhost:9000', access_key=access_key, secret_key=secret_key, secure=False)
    for local_file in glob.glob(local_path + '/**'):
        local_file = local_file.replace(os.sep, "/") # Replace \ with / on Windows
        if not os.path.isfile(local_file):
            send_folder_to_s3(
                local_file, bucket_name, minio_path + "/" + os.path.basename(local_file))
        else:
            remote_path = os.path.join(
                minio_path, local_file[1 + len(local_path):])
            remote_path = remote_path.replace(
                os.sep, "/")  # Replace \ with / on Windows
            client.fput_object(bucket_name, remote_path, local_file)
