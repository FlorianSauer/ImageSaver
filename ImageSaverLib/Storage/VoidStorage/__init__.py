from typing import Optional

from ImageSaverLib.Encapsulation import WrappingType
from ImageSaverLib.MetaDB.Types.Resource import ResourceSize
from ImageSaverLib.Storage.StorageBuilder import StorageBuilderInterface, str_to_bool, str_to_bytesize
from ..StorageInterface import AbstractSizableStorageInterface, StorageSize


class VoidStorage(AbstractSizableStorageInterface, StorageBuilderInterface):
    __storage_name__ = 'void'

    def __init__(self, debug=False, wrap_type=None, max_resource_size=None, max_storage_size=None):
        # type: (bool, Optional[WrappingType], Optional[ResourceSize], Optional[StorageSize]) -> None
        AbstractSizableStorageInterface.__init__(self, debug, wrap_type, max_resource_size, max_storage_size)
        self.name_counter = 0

    def _calculateCurrentSize(self):
        return self.name_counter

    def identifier(self):
        return '<VoidStorage at ' + repr(hex(hash(self))) + '>'

    @classmethod
    def loadRessource(cls, resource_name):
        return b''

    def saveResource(self, resource_data, resource_hash, resource_size):
        self.name_counter += 1
        self.increaseCurrentSize(1)
        return str(self.name_counter)

    def deleteResource(self, resource_name):
        self.resetCurrentSize()

    def listResourceNames(self):
        return [str(i) for i in range(self.name_counter)]

    def wipeResources(self):
        self.resetCurrentSize()

    @classmethod
    def build(cls, debug='False', wrap_type=None, max_resource_size=None, max_storage_size=None):
        debug = str_to_bool(debug)
        if max_resource_size:
            max_resource_size = str_to_bytesize(max_resource_size)
        if max_storage_size:
            max_storage_size = str_to_bytesize(max_storage_size)
        return cls(debug, wrap_type, max_resource_size, max_storage_size)
