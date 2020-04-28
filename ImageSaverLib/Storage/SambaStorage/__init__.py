from typing import Optional

from ..StorageInterface import AbstractSizableStorageInterface, StorageSize
from ..StorageBuilder import StorageBuilderInterface, str_to_bool, str_to_bytesize
from ..FileSystemStorage import FileSystemStorage2
from .Connector.SambaFileSystemConnector import SambaFileSystemConnector
from ...Encapsulation.Wrappers import WrappingType
from ...MetaDB.Types.Resource import ResourceSize


class SambaStorage(AbstractSizableStorageInterface, StorageBuilderInterface):
    __storage_name__ = 'samba'

    def __init__(self, user_id, password, server_ip, server_name=None, client_machine_name='imagesaver',
                 extension='png', service_name='ImageSaver', directory='isl_storage', debug=False, folder_depth=1,
                 folder_max_items=1000, wrap_type=None, max_resource_size=None, max_storage_size=None):
        # type: (str, str, str, Optional[str], str, str, str, str, bool, int, int, Optional[WrappingType], Optional[ResourceSize], Optional[StorageSize]) -> None
        AbstractSizableStorageInterface.__init__(self, debug, wrap_type, max_resource_size, max_storage_size)
        self.backend = SambaFileSystemConnector(user_id, password, server_ip, server_name, client_machine_name,
                                                service_name)
        self.structurizer = FileSystemStorage2(directory, extension, debug, self.backend, folder_depth,
                                               folder_max_items)

    def _calculateCurrentSize(self):
        return self.backend.treeSize('/')

    def identifier(self):
        return self.structurizer.identifier()

    @classmethod
    def build(cls, username, password, host, service='ImageSaver', directory='isl_storage', extension='bin',
              debug='False', depth='1', max_items='1000', wrap_type=None,
              max_resource_size=None, max_storage_size=None):
        debug = str_to_bool(debug)
        depth = int(depth)
        max_items = int(max_items)
        if max_resource_size:
            max_resource_size = str_to_bytesize(max_resource_size)
        if max_storage_size:
            max_storage_size = str_to_bytesize(max_storage_size)
        return cls(user_id=username, password=password, server_ip=host, extension=extension, service_name=service,
                   directory=directory, debug=debug, folder_depth=depth, folder_max_items=max_items,
                   wrap_type=wrap_type, max_resource_size=max_resource_size)

    def loadRessource(self, resource_name):
        return self.structurizer.loadRessource(resource_name)

    def saveResource(self, resource_data, resource_hash, resource_size):
        resource_name = self.structurizer.saveResource(resource_data, resource_hash, resource_size)
        self.increaseCurrentSize(resource_size)
        return resource_name

    def deleteResource(self, resource_name):
        self.structurizer.deleteResource(resource_name)
        self.resetCurrentSize()

    def listResourceNames(self):
        return self.structurizer.listResourceNames()

    def wipeResources(self):
        self.structurizer.wipeResources()
        self.resetCurrentSize()
