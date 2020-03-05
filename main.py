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

def gen_bin(filename,size):
    """
    generate big binary file with the specified size in bytes
    https://www.bswen.com/2018/04/python-How-to-generate-random-large-file-using-python.html
    :param filename: the filename
    :param size: the size in bytes
    :return:void
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'wb') as fout:
        fout.write(os.urandom(size))

def gen_text(filename,size):
    """
    generate big random letters/alphabets to a file
    :param filename: the filename
    :param size: the size in bytes
    :return: void
    """
    import random
    import string

    chars = ''.join([random.choice(string.ascii_letters) for i in range(size)]) #1

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, 'w') as f:
        f.write(chars)

def del_and_make_dir(dirname):
    if os.path.exists(dirname):
        shutil.rmtree(dirname)
    Path(dirname).mkdir(parents=True, exist_ok=True)

def gen_files():
    del_and_make_dir(OUTPUT_DIR)
    del_and_make_dir(INPUT_DIR)
    gen_bin("input/simple/foo", 10)
    gen_text("input/simple/bar", 10)
    gen_bin("input/big/foo", 10**9)
    gen_text("input/big/bar", 10)
    # generate_big_random_bin_file(x, y for x, y in FILES.items())


gen_files()

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