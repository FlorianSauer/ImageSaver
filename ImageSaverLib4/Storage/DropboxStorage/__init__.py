from typing import Optional

from .Connector.DropboxFileSystemConnector import DropboxFileSystemConnector
from ..FileSystemStorage import FileSystemStorage2
from ..StorageInterface import AbstractSizableStorageInterface, StorageSize
from ..StorageBuilder import StorageBuilderInterface, str_to_bool, str_to_bytesize
from ...Encapsulation.Wrappers import WrappingType
from ...MetaDB.Types.Resource import ResourceSize


class DropboxStorage(AbstractSizableStorageInterface, StorageBuilderInterface):
    __storage_name__ = 'dropbox'
    DEFAULT_MAX_STORAGE_SIZE = None

    def __init__(self, token, extension='bin', directory='/', folder_depth=1, folder_max_items=1000, debug=False,
                 wrap_type=None, max_resource_size=None, max_storage_size=None):
        # type: (str, str, str, int, int, bool, Optional[WrappingType], Optional[ResourceSize], Optional[StorageSize]) -> None
        AbstractSizableStorageInterface.__init__(self, debug=debug, wrap_type=wrap_type, max_resource_size=max_resource_size,
                                         max_storage_size=max_storage_size)
        self.backend = DropboxFileSystemConnector(token)
        self.structurizer = FileSystemStorage2(directory, extension, debug, self.backend, folder_depth,
                                               folder_max_items)

    def _calculateCurrentSize(self):
        return self.backend.currentSize()

    def _calculateTotalSize(self):
        orig_total = self.backend.totalSize()
        orig_used = self.backend.currentSize()
        saver_used = self.backend.treeSize('/')
        other_used = orig_used - saver_used
        return orig_total - other_used

    @classmethod
    def build(cls, token, extension='bin', directory='/', depth='1', max_items='1000', debug='False', wrap_type=None,
              max_resource_size=None, max_storage_size=None):
        debug = str_to_bool(debug)
        depth = int(depth)
        max_items = int(max_items)
        if max_resource_size:
            max_resource_size = str_to_bytesize(max_resource_size)
        if max_storage_size:
            max_storage_size = str_to_bytesize(max_storage_size)

        return cls(token=token, extension=extension, directory=directory, folder_depth=depth,
                   folder_max_items=max_items, debug=debug, wrap_type=wrap_type, max_resource_size=max_resource_size,
                   max_storage_size=max_storage_size)

    def identifier(self):
        return self.structurizer.identifier()

    def loadRessource(self, resource_name):
        return self.structurizer.loadRessource(resource_name)

    def saveResource(self, resource_data, resource_hash, resource_size):
        resource_name = self.structurizer.saveResource(resource_data, resource_hash, resource_size)
        self.increaseCurrentSize(resource_size)
        return resource_name

    def deleteResource(self, resource_name):
        self.resetCurrentSize()
        return self.structurizer.deleteResource(resource_name)

    def listResourceNames(self):
        return self.structurizer.listResourceNames()

    def wipeResources(self):
        self.resetCurrentSize()
        return self.structurizer.wipeResources()
