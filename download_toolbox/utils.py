import datetime as dt
import logging
import subprocess as sp

from functools import wraps

import dask
from dask.distributed import Client, LocalCluster


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
                 workers: int = 8):

        self._dashboard_port = dask_port
        self._timeout = dask_timeouts
        self._tmp_dir = dask_tmp_dir
        self._workers = workers

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
        }):
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
