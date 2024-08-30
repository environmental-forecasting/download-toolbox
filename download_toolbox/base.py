from abc import abstractmethod, ABCMeta
import logging
import os
import shutil

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
                 *,
                 identifier: str,
                 base_path: str = os.path.join(".", "data"),
                 config_type: str = "data_collection",
                 path_components: list = None,
                 **kwargs) -> None:
        self._identifier = identifier

        path_components = list() if path_components is None else path_components
        if not isinstance(path_components, list):
            raise DataCollectionError("path_components should be an Iterator")

        # TODO: seriously: root_path, path and base_path!? Rationalise this, too smelly for words
        self._base_path = base_path
        self._path_components = path_components
        self._root_path = None
        self._path = None
        self._config = None
        self._config_type = config_type

        self.init()

    def copy_to(self, new_identifier: object, base_path: os.PathLike = None):
        """

        Args:
            new_identifier:
            base_path:
        """
        old_path = self.path

        if base_path is not None:
            logging.info("Setting base path for copy to {}".format(base_path))
            self._base_path = base_path

        self.identifier = new_identifier

        logging.info("Copying {} to {}".format(old_path, self.path))
        shutil.copytree(old_path, self.path, dirs_exist_ok=True)

    def get_config(self,
                   config_funcs: dict = None,
                   strip_keys: list = None) -> dict:
        """get_config returns the implementation configuration for re-instantiation

        get_config returns a configuration dictionary that provides not just a reference
        but also a portability layer for recreating classes.

        For things that aren't serialisable natively, use config_funcs to serialise or represent
        values that allow recreation (it's on you to recreate those appropriately). An example
        is available at ...download_toolbox.interface.get_dataset_config_implementation

        If you supply any arguments in a derived implementation, use strip_keys to prevent
        them being exported into configurations that would then result in duplicate arguments
        when the class is recreated from config

        TODO: documenting get_config in derived implementations
        TODO: schema and validation for this library and others, helping to control implementations
         to aid portability of pipelines

        Args:
            config_funcs:
            strip_keys:

        Returns:

        """
        strip_keys = [] if strip_keys is None else strip_keys
        return {k: config_funcs[k](v) if config_funcs is not None and k in config_funcs else v
                for k, v in self.__dict__.items() if k not in ["_path", "_config", "_root_path"] + strip_keys}

    def init(self):
        self._config = None
        self._root_path = os.path.join(self._base_path, self._identifier)
        self._path = os.path.join(self._root_path, *self._path_components)

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

    def save_config(self):
        saved_config = self.config.render(self)
        logging.info("Saved dataset config {}".format(saved_config))
        return saved_config

    @property
    def base_path(self):
        return self._base_path

    @property
    def config(self):
        if self._config is None:
            self._config = Configuration(directory=self.root_path,
                                         config_type=self._config_type,
                                         identifier=self.identifier)
        return self._config

    @property
    def config_file(self):
        return self.config.output_file

    @property
    def config_type(self):
        return self._config_type

    @property
    def identifier(self) -> str:
        """The identifier (label) for this data collection."""
        return self._identifier

    @identifier.setter
    def identifier(self, identifier: str) -> None:
        self._identifier = identifier
        self.init()

    @property
    def path(self) -> str:
        """The base path of the data collection."""
        return self._path

    @path.setter
    def path(self, path: str) -> None:
        self._path = path

    @property
    def path_components(self):
        return self._path_components

    @property
    def root_path(self):
        return self._root_path


#    def __repr__(self):
#        return "{} with path {}".format(self.name, self.path)


class DataCollectionError(RuntimeError):
    pass


