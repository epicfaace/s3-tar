# s3-tar

Demo of storing a file directory in S3 and retrieving individual files.

## Functions

- `upload_file` - tar's a directory and uploads this tar to S3 (as `{bundleId}/bundle.tar`). Also, the tar header blocks for all files are concatenated together and uploaded to S3 as a separate file (`{bundleId}/metadata.bin`).

- `download_all` - downloads the tar file (`{bundleId}/bundle.tar`) from S3 and then unzips it to the `output` directory.

- `download_single` - downloads a single file from within the bundle tar from S3 and writes it to the `output` directory. This is where the magic happens:
    - Downloads `{bundleId}/metadata.bin` and retrieves the file position and size of the desired file.
    - Makes an S3 request for `{bundleId}/bundle.tar`, but use the `Range` header to only retrieve the contents of the desired file.
    - Make a new tar archive with the header of the desired file and contents of the desired file, then extract this archive to get the original file.

- `list_files` (not implemented yet) - list files in a directory -- this should just involve reading `{bundleId}/metadata.bin` as before.


## Benchmarks

The point is that `download_single` is far faster than `download_all`. The `big` test below has two files: a 100 B file called "small" and a 1 GB file called "big". It takes over a minute  to extract the entire archive, but if you just want to preview the "small" file in the archive, it takes less than a second.

```
starting test, test_name=big, test_type=download_all, single_file_name=None
	creating tar file...
	took 2.6516146659851074
	uploading tar file...
	took 10.513373374938965
	downloading tar file...
	took 66.95749759674072
starting test, test_name=big, test_type=download_single, single_file_name=small
	skipping creation of tar file.
	uploading tar file...
	took 9.734010696411133
	downloading tar file...
	file: input/big
	file: input/big/bar
	file: input/big/small
	took 0.2481975555419922
```

## Running locally

```
rm -rf input output
pipenv install
pipenv shell
python main.py
```

It takes a while to create the tar files in the `input` directory, so if you want to reuse these files in future runs, just don't delete the `input` directory after the first run and comment out `gen_files()` in `main.py`.