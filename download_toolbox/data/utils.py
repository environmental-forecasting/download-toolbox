import boto3
import collections
import logging
import os

import pandas as pd
import xarray as xr

from botocore import UNSIGNED
from botocore.config import Config
from typing import Union


def batch_requested_dates(dates: object,
                          attribute: str = "month") -> object:
    """

    TODO: should be using Pandas DatetimeIndexes / Periods for this, but the
     need to refactor slightly, and this is working for the moment

    TODO: we should be yielding from here surely

    :param dates:
    :param attribute:
    :return:
    """
    dates = collections.deque(sorted(dates))

    logging.debug("Got {} dates to batch".format(len(dates)))
    batched_dates = []
    batch = []

    while len(dates):
        if not len(batch):
            batch.append(dates.popleft())
        else:
            if getattr(batch[-1], attribute) == getattr(dates[0], attribute):
                batch.append(dates.popleft())
            else:
                logging.debug("Appending batch of length {}".format(len(batch)))
                batched_dates.append(batch)
                batch = []

    if len(batch):
        batched_dates.append(batch)

    if len(dates) > 0:
        raise RuntimeError("Batching didn't work!")

    logging.debug("Return {} batches for {} batch".format(len(batched_dates), attribute))
    return batched_dates


def merge_files(new_datafile: object,
                other_datafile: object,
                drop_variables: object = None):
    """

    :param new_datafile:
    :param other_datafile:
    :param drop_variables:
    """
    drop_variables = list() if drop_variables is None else drop_variables

    if other_datafile is not None:
        (datafile_path, new_filename) = os.path.split(new_datafile)
        moved_new_datafile = \
            os.path.join(datafile_path, "new.{}".format(new_filename))
        os.rename(new_datafile, moved_new_datafile)
        d1 = xr.open_dataarray(moved_new_datafile,
                               drop_variables=drop_variables)

        logging.debug("Concatenating with previous data {}".format(
            other_datafile
        ))
        d2 = xr.open_dataarray(other_datafile,
                               drop_variables=drop_variables)

        # Ordering is important for merging, place the temporary over the existing
        # as it will ensure that code updates and other changes are updated
        new_ds = xr.concat([d2, d1], dim="time").\
            sortby("time").\
            drop_duplicates("time", keep="first")

        logging.info("Saving merged data to {}... ".
                     format(new_datafile))
        new_ds.to_netcdf(new_datafile)
        os.unlink(other_datafile)
        os.unlink(moved_new_datafile)


def s3_file_download(bucket_name: str, key: str, filename: str) -> None:
    """
    Download a file from S3 bucket to local storage.

    If the file already exists and matches the expected size, skip download.
    If the file exists but is incomplete, re-download it.

    Args:
        bucket_name: Name of the S3 bucket.
        key: Key of the file in the S3 bucket.
        filename: Local path to save the downloaded file.
    """
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    # Get file size from S3 bucket
    try:
        metadata = s3.head_object(Bucket=bucket_name, Key=key)
        s3_file_size = metadata['ContentLength']
    except Exception as e:
        logging.error(f"Error getting metadata: {e}")
        return

    # Check if local file exists and matches the expected size
    if os.path.exists(filename):
        local_file_size = os.path.getsize(filename)
        if local_file_size == s3_file_size:
            logging.info(f"File already downloaded: {filename}")
            return
        else:
            logging.info(f"Incomplete file found (local: {local_file_size}, remote: {s3_file_size}). Re-downloading...")

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "wb") as f:
        s3.download_fileobj(bucket_name, key, f)
    logging.info(f"Download complete: {filename}")


def xr_save_netcdf(da: xr.DataArray, file_path: str, complevel: int = 0) -> None:
    """
    Save xarray Dataarray to netCDF file with optional compression.

    Args:
        da: The xarray dataarray to be output to netCDF.
        file_path: Path to save the netCDF file.
        complevel (optional): Level of compression to apply.
                              Defaults to 0.
    """
    if complevel:
        compression = dict(zlib=True, complevel=int(complevel))
        var_encoding = {da.name: compression}
        coords_encoding = {coord: compression for coord in da.coords}
        da.to_netcdf(file_path, mode="w", encoding=var_encoding | coords_encoding)
    else:
        var_encoding = {da.name: {}}
        coords_encoding = {coord: {} for coord in da.coords}
        da.to_netcdf(file_path, mode="w", encoding=var_encoding | coords_encoding)
