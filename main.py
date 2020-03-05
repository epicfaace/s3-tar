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
from time import time
from io import BytesIO
import gzip

BUCKET_NAME = "codalab-test"
AWS_PROFILE_NAME = "codalab-test"
session = boto3.Session(profile_name=AWS_PROFILE_NAME)
s3_client = session.client("s3")
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

def gen_sparse(filename,size):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    f = open(filename, "wb")
    f.seek(size - 1)
    f.write(b'\x00')
    f.close()
    pass

def del_and_make_dir(dirname):
    if os.path.exists(dirname):
        shutil.rmtree(dirname)
    Path(dirname).mkdir(parents=True, exist_ok=True)

def gen_files():
    print("gen files...")
    del_and_make_dir(OUTPUT_DIR)
    del_and_make_dir(INPUT_DIR)
    gen_text("input/simple/foo", 100)
    gen_text("input/simple/bar", 100)
    gen_text("input/big/small", 100)
    # gen_sparse("input/big/bar", 10 * 10**9) # 10 GB
    gen_sparse("input/big/bar", 1 * 10**9) # 1 GB
    print("done gen files")


gen_files()

class S3TestBase:
    test_name = None
    test_type = None
    single_file_name = None
    def setUp(self):
        print(f"starting test, test_name={self.test_name}, test_type={self.test_type}, single_file_name={self.single_file_name}")
    def upload_file(self, input_dir, tar_file, key, metadata_key):
        init_time = time()
        if os.path.exists(tar_file):
            print("\tskipping creation of tar file.")
        else:
            print("\tcreating tar file...")
            with tarfile.open(tar_file, "w:") as tar:
                tar.add(input_dir)
            print(f"\ttook {time() - init_time}")
        
        tar_metadata_fileobj = BytesIO()
        tarinfos = []
        with tarfile.open(tar_file, "r:") as tar:
            for tarinfo in tar:
                assert(tarinfo.offset + 512 == tarinfo.offset_data)
                tar_metadata_fileobj.write(tarinfo.tobuf())
        tar_metadata_fileobj.seek(0)

        init_time = time()
        print("\tuploading tar file...")
        response = s3_client.upload_file(tar_file, BUCKET_NAME, key)
        s3_client.upload_fileobj(tar_metadata_fileobj, BUCKET_NAME, metadata_key)
        print(f"\ttook {time() - init_time}")

    def download_all(self, output_dir, key):
        init_time = time()
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key)
        with tarfile.open(fileobj=response["Body"], mode='r|') as tar:
            tar.extractall(output_dir + "/" + self.test_type)
    
    def download_single(self, output_dir, key, metadata_key, single_file_path):
        tar_metadata_fileobj = BytesIO()
        s3_client.download_fileobj(BUCKET_NAME, metadata_key, tar_metadata_fileobj)
        tar_metadata_fileobj.seek(0)
        found_tarinfo = None
        offset = 0
        while True:
            data = tar_metadata_fileobj.read(512)
            if len(data) == 0: break
            tarinfo = tarfile.TarInfo.frombuf(data, encoding=tarfile.ENCODING, errors="")
            print(f"\tfile: {tarinfo.name}")
            # assert(tarinfo.offset + 512 == tarinfo.offset_data)
            if tarinfo.name == single_file_path:
                found_tarinfo = tarinfo
                offset += 512
                break
            offset += 512 + tarinfo.size

        if not found_tarinfo:
            print(f"tarinfo not found for file: {single_file_path}")
        
        range_ = "bytes={}-{}".format(offset, offset + found_tarinfo.size - 1)
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=key, Range=range_)
        data = response["Body"].read()
        found_fileobj = BytesIO(data)
        found_fileobj.seek(0)
        tarfile_obj = BytesIO()
        with tarfile.open(fileobj=tarfile_obj, mode='w:') as tar:
            tar.addfile(found_tarinfo, found_fileobj)
        # Extract tar
        tarfile_obj.seek(0)
        with tarfile.open(fileobj=tarfile_obj, mode='r:') as tar:
            for tarinfo in tar:
                assert(tarinfo.offset + 512 == tarinfo.offset_data)
            tar.extractall(output_dir + "/" + self.test_type)
    
    def test_run(self):
        bundleid = str(uuid.uuid4())
        input_dir = INPUT_DIR + "/" + self.test_name
        output_dir = OUTPUT_DIR + "/" + self.test_name
        tar_file = INPUT_DIR + "/" + self.test_name + ".tar"
        key = bundleid + "/bundle.tar"
        metadata_key = bundleid + "/metadata.bin"
        self.upload_file(input_dir, tar_file, key, metadata_key)
        init_time = time()
        print("\tdownloading tar file...")
        if self.test_type == "download_all":
            self.download_all(output_dir, key)
            cmp = filecmp.dircmp(input_dir, output_dir + "/" + self.test_type).diff_files
            self.assertEqual(len(cmp), 0)
        elif self.test_type == "download_single":
            self.download_single(output_dir, key, metadata_key, input_dir + "/" + self.single_file_name)
        print(f"\ttook {time() - init_time}")
        init_time = time()



class S3BasicTest(S3TestBase, unittest.TestCase):
    test_name = "simple"
    test_type = "download_all"

class S3BasicTestSingle(S3TestBase, unittest.TestCase):
    test_name = "simple"
    test_type = "download_single"
    single_file_name = "foo"

class S3BigTest(S3TestBase, unittest.TestCase):
    test_name = "big"
    test_type = "download_all"

class S3BigTestSingle(S3TestBase, unittest.TestCase):
    test_name = "big"
    test_type = "download_single"
    single_file_name = "bar"

class S3BigTestSingle(S3TestBase, unittest.TestCase):
    test_name = "big"
    test_type = "download_single"
    single_file_name = "small"

if __name__ == "__main__":
    unittest.main()