from typing import cast, List, Optional

from ImageSaverLib4.Encapsulation import WrappingType
from ImageSaverLib4.MetaDB.Types.Resource import ResourceSize
from ._RamStorage import RamStorage as _RamStorage
from ..Errors import NotFoundError
from ..StorageBuilder import StorageBuilderInterface, str_to_bool, str_to_bytesize
from ..StorageInterface import AbstractSizableStorageInterface, StorageSize
from ...MetaDB.Types.Resource import ResourceName


class RamStorage(AbstractSizableStorageInterface, StorageBuilderInterface):
    __storage_name__ = 'memory'

    def __init__(self, debug=False, wrap_type=None, max_resource_size=None, max_storage_size=None):
        # type: (bool, Optional[WrappingType], Optional[ResourceSize], Optional[StorageSize]) -> None
        AbstractSizableStorageInterface.__init__(self, debug, wrap_type, max_resource_size, max_storage_size)
        self.storage = _RamStorage()

    def _calculateCurrentSize(self):
        return self.storage.size()

    def identifier(self):
        return '<_RamStorage at ' + repr(hex(hash(self))) + '>'

    def deleteResource(self, resource_name):  # type: (ResourceName) -> None
        # (self, abstractDataFragmentInfo, name=None):
        self.debugPrint("remove resource", resource_name)
        self.storage.delete(resource_name)
        self.resetCurrentSize()

    def wipeResources(self):
        self.debugPrint("---wipe---")
        self.storage.wipe()
        self.resetCurrentSize()

    def saveResource(self, resource_data, resource_hash, resource_size):
        name = resource_hash.hex()
        self.debugPrint("saving resource @", name)
        self.storage.add(name, resource_data)
        self.increaseCurrentSize(len(resource_data))
        return name

    def loadRessource(self, resource_name):
        self.debugPrint("loading Resource @", resource_name)
        try:
            return self.storage.load(resource_name)
        except KeyError:
            pass
        raise NotFoundError("requested resource not found/saved")

    def listResourceNames(self):
        self.debugPrint("load fragment names")
        return cast(List[ResourceName], self.storage.list())

    @classmethod
    def build(cls, debug='False', wrap_type=None, max_resource_size=None, max_storage_size=None):
        debug = str_to_bool(debug)
        if max_resource_size:
            max_resource_size = str_to_bytesize(max_resource_size)
        if max_storage_size:
            max_storage_size = str_to_bytesize(max_storage_size)
        return cls(debug, wrap_type, max_resource_size, max_storage_size)
