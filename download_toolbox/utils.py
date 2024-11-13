import datetime as dt
import ftplib
import importlib
import logging
import subprocess as sp
import sys
import threading
from ftplib import FTP

from functools import wraps

import dask
import requests
from dask.distributed import Client, LocalCluster


def get_implementation(location):
    if ":" not in location:
        if hasattr(sys.modules[__name__], location):
            return getattr(sys.modules[__name__], location)
        else:
            raise ImportError("There is no {} available in sys.modules[__name__] "
                              "and no module path provided".format(location))
    module_ref, object_name = location.split(":")
    implementation = None

    try:
        module = importlib.import_module(module_ref)
        implementation = getattr(module, object_name)
    except ImportError:
        logging.exception("Unable to import from location: {}".format(location))

    return implementation


def run_command(command: str, dry: bool = False):
    """Run a shell command

    A wrapper in case we want some additional handling to go in here

    :param command:
    :param dry:
    :return:

    """
    if dry:
        logging.info("Skipping dry commaand: {}".format(command))
        return 0

    ret = sp.run(command, shell=True)
    if ret.returncode < 0:
        logging.warning("Child was terminated by signal: {}".
                        format(-ret.returncode))
    else:
        logging.info("Child returned: {}".format(-ret.returncode))

    return ret


def setup_logging(func,
                  log_format="[%(asctime)-17s :%(levelname)-8s] - %(message)s"):
    @wraps(func)
    def wrapper(*args, **kwargs):
        parsed_args = func(*args, **kwargs)
        level = logging.INFO

        if hasattr(parsed_args, "verbose") and parsed_args.verbose:
            level = logging.DEBUG

        logging.basicConfig(
            level=level,
            format=log_format,
            datefmt="%d-%m-%y %T",
        )
        # FIXME: something is interrupting the root logger setup
        logging.getLogger().setLevel(level)
        # TODO: better way of handling these on a case by case basis
        logging.getLogger("cdsapi").setLevel(logging.WARNING)
        logging.getLogger("requests").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        return parsed_args
    return wrapper


# This is adapted from the data/loaders implementations
class DaskWrapper:
    """

    :param dask_port:
    :param dask_timeouts:
    :param dask_tmp_dir:
    :param workers:
    """

    def __init__(self,
                 dask_port: int = 8888,
                 dask_timeouts: int = 60,
                 dask_tmp_dir: object = "/tmp",
                 workers: int = 8,
                 scheduler: str = "single-threaded",
                 ):

        self._dashboard_port = dask_port
        self._timeout = dask_timeouts
        self._tmp_dir = dask_tmp_dir
        self._workers = workers
        self._scheduler = scheduler

    def dask_process(self,
                     *args,
                     method: callable,
                     **kwargs):
        """

        :param method:
        """
        dashboard = "localhost:{}".format(self._dashboard_port)

        with dask.config.set({
            "temporary_directory": self._tmp_dir,
            "distributed.comm.timeouts.connect": self._timeout,
            "distributed.comm.timeouts.tcp": self._timeout,
            # "scheduler": self._scheduler, # Fix to "single-threaded" for netCDF4 >=1.6.1 not thread-safe.
        }
        ):
            cluster = LocalCluster(
                dashboard_address=dashboard,
                n_workers=self._workers,
                threads_per_worker=1,
                scheduler_port=0,
            )
            logging.info("Dashboard at {}".format(dashboard))

            with Client(cluster) as client:
                logging.info("Using dask client {}".format(client))
                ret = method(*args, **kwargs)
        return ret


class FTPClient(object):
    def __init__(self,
                 host: str,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._ftp = None
        self._ftp_host = host
        self._cache = dict()
        self._ftp_connections = dict()

    def single_request(self,
                       source_dir: object,
                       source_filename: list,
                       destination_path: object):
        thread_id = threading.get_native_id()
        if threading.get_native_id() not in self._ftp_connections:
            logging.debug("FTP opening for thread {}".format(thread_id))
            self._ftp_connections[thread_id] = FTP(self._ftp_host)
            ftp_connection = self._ftp_connections[thread_id]
            ftp_connection.login()
        else:
            ftp_connection = self._ftp_connections[thread_id]

        try:
            logging.debug("FTP changing to {}".format(source_dir))
            # self._ftp.cwd(source_dir)

            if source_dir not in self._cache:
                self._cache[source_dir] = ftp_connection.nlst(source_dir)

            ftp_files = [el for el in self._cache[source_dir] if el.endswith(source_filename)]
            if not len(ftp_files):
                raise ClientError("File is not available: {}".format(source_filename))
        except ftplib.error_perm as e:
            raise ClientError("FTP error, possibly missing directory {}: {}".format(source_dir, e))

        logging.debug("FTP Attempting to retrieve to {} from {}".format(destination_path, ftp_files[0]))
        with open(destination_path, "wb") as fh:
            ftp_connection.retrbinary("RETR {}".format(ftp_files[0]), fh.write)


class HTTPClient(object):
    def __init__(self,
                 host: str,
                 *args,
                 source_base: object = None,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self._host = host
        self._source_base = source_base

    def single_request(self,
                       source: object,
                       destination_path: object,
                       method: str = "get",
                       request_options: dict = None):
        request_options = dict() if request_options is None else request_options
        source_url = "/".join([self._host, self._source_base, source])

        try:
            logging.debug("{}-ing {} with {}".format(method, source_url, request_options))
            response = getattr(requests, method)(source_url, **request_options)
        except requests.exceptions.RequestException as e:
            raise ClientError("HTTP error {}: {}".format(source_url, e))

        if hasattr(response, "status_code") and response.status_code == 200:
            logging.debug("Attempting to output response content to {}".format(destination_path))
            with open(destination_path, "wb") as fh:
                fh.write(response.content)
        else:
            raise ClientError("HTTP response was not successful, writing nothing: {}".format(response.status_code))


class ClientError(RuntimeError):
    pass
