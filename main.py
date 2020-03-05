"""
tarfile attrs (.offset, .offset_data): https://github.com/python/cpython/blob/master/Lib/tarfile.py#L728-L732

"""

import boto3
import unittest
import os
from pathlib import Path
import tarfile
import uuid

BUCKET_NAME = "codalab-test"
AWS_PROFILE_NAME = "codalab-test"
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
s3_client = session.client("s3")
TAR_FILE = "out.tar.gz"
INPUT_DIR = "input"
OUTPUT_DIR = "output"

class S3Test(unittest.TestCase):
    def setUp(self):
        if os.path.exists(TAR_FILE):
            os.remove(TAR_FILE)
        if os.path.exists(OUTPUT_DIR):
            os.rmdir(OUTPUT_DIR)
        Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    def tearDown(self):
        self.setUp()
    def upload_file(self, file_name, key):
        response = s3_client.upload_file(file_name, BUCKET_NAME, key)
    def download_file(self, key):
        print("downloading file:")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        # Range=...
        # https://kokes.github.io/blog/2018/07/26/s3-objects-streaming-python.html
        with tarfile.open(fileobj=response["Body"], mode='r|gz') as tar:
            for tarinfo in tar:
                print(tarinfo.name)
    def test_basic(self):
        bundleid = str(uuid.uuid4())
        with tarfile.open(TAR_FILE, "w:gz") as tar:
            for name in ["input/foo", "input/bar", "input/quux"]:
                tar.add(name)
        self.upload_file(TAR_FILE, bundleid + "/" + TAR_FILE)
        self.download_file(bundleid + "/" + TAR_FILE)
        # with tarfile.open(TAR_FILE, "r:gz") as tar:
        #     for tarinfo in tar:
        #         print(tarinfo.offset)
        #         # tarinfo.offset
        #         # tarinfo.offset_data

if __name__ == "__main__":
    unittest.main()