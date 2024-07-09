from abc import abstractmethod, ABCMeta
import logging
import os

from download_toolbox.config import Configuration


class DataCollection(metaclass=ABCMeta):
    """An Abstract base class with common interface for data collection classes.

    It represents a collection of data assets on a filesystem, though in the future
    it would make sense that we also allow use for object storage etc.

    This also handles automatic egress/ingress and validation of the configurations
    for these collections.

    :param _identifier: The identifier of the data collection.
    :param _path: The base path of the data collection.
    :raises AssertionError: Raised if identifier is not specified, or no hemispheres are selected.
    """

    @abstractmethod
    def __init__(self,
                 *args,
                 identifier: str,
                 base_path: str = os.path.join(".", "data"),
                 path_components: object = None,
                 **kwargs) -> None:
        self._identifier: str = identifier

        path_components = list() if path_components is None else path_components
        if not isinstance(path_components, list):
            raise DataCollectionError("path_components should be an Iterator")
        self._base_path = base_path
        self._path = os.path.join(base_path, identifier, *path_components)
        self._root_path = os.path.join(base_path, identifier)

        if self._identifier is None:
            raise DataCollectionError("No identifier supplied")

        if os.path.exists(self._path):
            logging.debug("{} already exists".format(self._path))
        else:
            if not os.path.islink(self._path):
                logging.info("Creating path: {}".format(self._path))
                os.makedirs(self._path, exist_ok=True)
            else:
                logging.info("Skipping creation for symlink: {}".format(self._path))

        self._config = None

    @property
    def config(self):
        if self._config is None:
            self._config = Configuration(directory=self.root_path, identifier=self.identifier)
        return self._config

    @staticmethod
    def open_config(config):
        logging.info("Opening dataset config {}".format(config))

        raise RuntimeError("This is not yet implemented, get working for preprocess-toolbox!")

    @property
    def path(self) -> str:
        """The base path of the data collection."""
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        self._path = path

    def get_config(self):
        return {k: v for k, v in self.__dict__.items() if k not in ["_config"]}

    @property
    def root_path(self):
        return self._root_path

    def save_config(self):
        saved_config = self.config.render(self)
        logging.info("Saved dataset config {}".format(saved_config))

    @property
    def identifier(self) -> str:
        """The identifier (label) for this data collection."""
        return self._identifier


#    def __repr__(self):
#        return "{} with path {}".format(self.name, self.path)


class DataCollectionError(RuntimeError):
    pass


