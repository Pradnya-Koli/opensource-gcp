"""Utility to upload to and download from GCS."""

import logging
import os
from os.path import abspath, basename, isdir, join
from pathlib import Path, PurePosixPath

from google.cloud import storage
from google.cloud.exceptions import NotFound
from google.cloud.storage import Bucket


def upload_directory(local_dir: str, bucket_name: str, gcs_path: str) -> None:
    """Uploads all the files from a local directory to a gcs bucket.

    Args:
      local_dir: path to the local directory.
      bucket_name: name of the gcs bucket.  If the bucket doesn't exist, the method
        tries to create one.
      gcs_path: the path to the gcs directory that stores the files.
    """
    assert isdir(local_dir), f"Can't find input directory {local_dir}."
    client = storage.Client()

    try:
        logging.info("Get bucket %s", bucket_name)
        bucket: Bucket = client.get_bucket(bucket_name)
    except NotFound:
        logging.info('The bucket "%s" does not exist, creating one...', bucket_name)
        bucket = client.create_bucket(bucket_name)

    dir_abs_path = abspath(local_dir)
    for root, _, files in os.walk(dir_abs_path):
        for name in files:
            sub_dir = root[len(dir_abs_path) :]
            if sub_dir.startswith("/") or sub_dir.startswith("\\"):
                sub_dir = sub_dir[1:]
            file_path = join(root, name)
            gcs_file_path = PurePosixPath(gcs_path, sub_dir, name)
            logging.info(
                'Uploading file "%s" to gcs "%s" ...', file_path, gcs_file_path
            )
            blob = bucket.blob(str(gcs_file_path))
            blob.upload_from_filename(file_path)
    logging.info(
        'Finished uploading input files to gcs "%s/%s".', bucket_name, gcs_path
    )


def download_directory(local_dir: str, bucket_name: str, gcs_path: str) -> None:
    """Download all the files from a gcs bucket to a local directory.

    Args:
        local_dir: path to the local directory to store the downloaded files. It will
            create the directory if it doesn't exist.
        bucket_name: name of the gcs bucket.
        gcs_path: the path to the gcs directory that stores the files.
    """
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=gcs_path)
    logging.info('Start downloading outputs from gcs "%s/%s"', bucket_name, gcs_path)
    for blob in blobs:
        file_name = basename(blob.name)
        sub_dir = Path(blob.name[len(gcs_path) + 1 : -len(file_name)])
        file_dir = Path(local_dir) / sub_dir
        os.makedirs(file_dir, exist_ok=True)
        file_path = file_dir / file_name
        logging.info('Downloading output file to "%s"...', file_path)
        blob.download_to_filename(file_path)

    logging.info('Finished downloading. Output files are in "%s".', local_dir)