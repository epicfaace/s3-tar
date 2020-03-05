"""
tarfile attrs (.offset, .offset_data): https://github.com/python/cpython/blob/master/Lib/tarfile.py#L728-L732

"""
import filecmp
import boto3
import unittest
import os
from pathlib import Path
import tarfile
import uuid
import shutil

BUCKET_NAME = "codalab-test"
AWS_PROFILE_NAME = "codalab-test"
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
s3_client = session.client("s3")
TAR_FILE = "out.tar.gz"
INPUT_DIR = "input"
OUTPUT_DIR = "output"

if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

class S3TestBase:
    def setUp(self):
        if os.path.exists(TAR_FILE):
            os.remove(TAR_FILE)
    def tearDown(self):
        self.setUp()
    def upload_file(self, file_name, key):
        response = s3_client.upload_file(file_name, BUCKET_NAME, key)
    def download_file(self, output_dir, key):
        print("downloading file:")
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        # Range=...
        # https://kokes.github.io/blog/2018/07/26/s3-objects-streaming-python.html
        with tarfile.open(fileobj=response["Body"], mode='r|gz') as tar:
            tar.extractall(output_dir)
    def test_run(self):
        bundleid = str(uuid.uuid4())
        key = bundleid + "/" + TAR_FILE
        input_dir = INPUT_DIR + "/" + self.test_name
        output_dir = OUTPUT_DIR + "/" + self.test_name

        with tarfile.open(TAR_FILE, "w:gz") as tar:
            tar.add(input_dir, arcname=self.test_type)
        self.upload_file(TAR_FILE, key)
        if self.test_type == "extractall":
            self.download_file(output_dir, key)

        cmp = filecmp.dircmp(input_dir, output_dir + "/" + self.test_type).diff_files
        self.assertEqual(len(cmp), 0)

class S3BasicTest(unittest.TestCase, S3TestBase):
    test_name = "simple"
    test_type = "extractall"

# class S3BigTest(unittest.TestCase, S3TestBase):
#     test_name = "big"
#     test_type = "extractall"

if __name__ == "__main__":
    unittest.main()