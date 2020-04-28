import os
from typing import Type, Union, Optional, cast

from .Connectors import FileSystemInterface
from .Connectors.LocalFileSystemConnector import LocalFileSystemConnector
from .FolderStructurizer import FolderStructurizer
from ..StorageInterface import AbstractSizableStorageInterface, StorageSize
from ..StorageBuilder import StorageBuilderInterface, str_to_bool, str_to_bytesize
from ...Encapsulation.Wrappers import WrappingType
from ...MetaDB.Types.Resource import ResourceSize


class FileSystemStorage2(AbstractSizableStorageInterface, StorageBuilderInterface):
    __storage_name__ = 'local'

    def __init__(self, directory, extension='bin', debug=False, backend=LocalFileSystemConnector, folder_depth=1,
                 folder_max_items=1000, wrap_type=None, max_resource_size=None, max_storage_size=None):
        # type: (str, str, bool, Union[Type[FileSystemInterface], FileSystemInterface], int, int, Optional[WrappingType], Optional[ResourceSize], Optional[StorageSize]) -> None
        if backend == LocalFileSystemConnector and max_storage_size is None:
            # print(directory)
            # print(backend.remaining(directory))
            max_storage_size = cast(LocalFileSystemConnector, backend).remaining(directory)
        AbstractSizableStorageInterface.__init__(self, debug, wrap_type, max_resource_size, max_storage_size)
        self.backend = backend
        self.structurizer = FolderStructurizer(self.backend, directory, extension, folder_depth, folder_max_items)

    def _calculateCurrentSize(self):
        return self.backend.treeSize(self.structurizer.root)

    def identifier(self):
        return '_d' + str(self.structurizer.folder_depth) + '_c' + str(
            self.structurizer.folder_max_items) + '_r' + self.structurizer.root + '_e' + self.structurizer.extension + '@' + self.backend.identifier()

    @classmethod
    def build(cls, directory, extension='bin', debug='False', depth='1', max_items='1000', wrap_type=None,
              max_resource_size=None, max_storage_size=None):
        directory = os.path.abspath(os.path.normpath(os.path.expanduser(directory)))
        # print(directory)
        debug = str_to_bool(debug)
        depth = int(depth)
        max_items = int(max_items)
        if max_resource_size:
            max_resource_size = str_to_bytesize(max_resource_size)
        if max_storage_size:
            max_storage_size = str_to_bytesize(max_storage_size)
        return cls(directory=directory, extension=extension, debug=debug, folder_depth=depth,
                   folder_max_items=max_items, wrap_type=wrap_type, max_resource_size=max_resource_size,
                   max_storage_size=max_storage_size)

    def loadRessource(self, resource_name):
        return self.structurizer.get(resource_name)

    def saveResource(self, resource_data, resource_hash, resource_size):
        resource_name = self.structurizer.add(resource_data, resource_hash, resource_size)
        self.increaseCurrentSize(resource_size)
        return resource_name

    def deleteResource(self, resource_name):
        self.resetCurrentSize()
        self.structurizer.delete(resource_name)

    def listResourceNames(self):
        return self.structurizer.list()

    def wipeResources(self):
        self.resetCurrentSize()
        self.structurizer.wipe()
