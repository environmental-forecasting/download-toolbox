from abc import abstractmethod, ABCMeta

import logging
import os

from download_toolbox.utils import Hemisphere, HemisphereMixin
from download_toolbox.config import Configuration


class DataCollection(HemisphereMixin, metaclass=ABCMeta):
    """An Abstract base class with common interface for data collection classes.

    :param _identifier: The identifier of the data collection.
    :param _path: The base path of the data collection.
    :param _hemisphere: The hemisphere(s) of the data collection.
    :raises AssertionError: Raised if identifier is not specified, or no hemispheres are selected.
    """

    @abstractmethod
    def __init__(self,
                 *args,
                 identifier: object = None,
                 north: bool = True,
                 south: bool = False,
                 path: str = os.path.join(".", "data"),
                 **kwargs) -> None:
        self._identifier: object = identifier
        self._path: str = os.path.join(path, identifier)
        self._hemisphere: Hemisphere = (Hemisphere.NORTH if north else Hemisphere.NONE) | \
                                       (Hemisphere.SOUTH if south else Hemisphere.NONE)

        assert self._identifier, "No identifier supplied"
        assert self._hemisphere != Hemisphere.NONE, "No hemispheres selected"

        self._config = Configuration(directory=self.base_path,
                                     identifier=self.identifier,
                                     location=self.hemisphere_loc)

    @property
    def base_path(self) -> str:
        """The base path of the data collection."""
        return self._path

    @base_path.setter
    def base_path(self, path: str) -> None:
        self._path = path
        self._config.render(path)

    @property
    def identifier(self) -> object:
        """The identifier (label) for this data collection."""
        return self._identifier


class DataProducer(DataCollection):
    """Manages the creation and organisation of data files.

    :param dry: Flag specifying whether the data producer should be in dry run mode or not.
    :param overwrite: Flag specifying whether existing files should be overwritten or not.
    """

    def __init__(self,
                 *args,
                 dry: bool = False,
                 overwrite: bool = False,
                 **kwargs) -> None:
        super(DataProducer, self).__init__(*args, **kwargs)

        self.dry = dry
        self.overwrite = overwrite

        if os.path.exists(self._path):
            logging.debug("{} already exists".format(self._path))
        else:
            if not os.path.islink(self._path):
                logging.info("Creating path: {}".format(self._path))
                os.makedirs(self._path, exist_ok=True)
            else:
                logging.info("Skipping creation for symlink: {}".format(
                    self._path))

        # NOTE: specific limitation for the DataProducers, they'll only do one
        # hemisphere per instance
        assert self._hemisphere != Hemisphere.BOTH, "Both hemispheres selected"

    def get_data_var_folder(self,
                            var: str,
                            append: object = None,
                            hemisphere: object = None,
                            missing_error: bool = False) -> str:
        """Returns the path for a specific data variable.

        Appends additional folders to the path if specified in the `append` parameter.

        :param var: The data variable.
        :param append: Additional folders to append to the path. Defaults to None.
        :param hemisphere: The hemisphere. Defaults to None.
        :param missing_error: Flag to specify if missing directories should be treated as an error. Defaults to False.
        :returns str: The path for the specific data variable.
        """
        if not append:
            append = []

        if not hemisphere:
            # We can make the assumption because this implementation is limited
            # to a single hemisphere
            hemisphere = self.hemisphere_str[0]

        data_var_path = os.path.join(self.base_path,
                                     *[hemisphere, var, *append])

        if not os.path.exists(data_var_path):
            if not missing_error:
                os.makedirs(data_var_path, exist_ok=True)
            else:
                raise OSError("Directory {} is missing and this is "
                              "flagged as an error!".format(data_var_path))

        return data_var_path


class Downloader(DataProducer):
    """Abstract base class for a downloader."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @abstractmethod
    def download(self):
        """Abstract download method for this downloader: Must be implemented by subclasses."""
        raise NotImplementedError("{}.download is abstract".format(
            __class__.__name__))
