from threading import Lock, RLock
from typing import Type, cast

from ..StorageInterface import StorageInterface, SizableStorageInterface


class SynchronizedStorage(StorageInterface):

    def __init__(self, storage, lock_type=Lock):
        # type: (StorageInterface, Type[Lock, RLock]) -> None
        super().__init__()
        self._storage = storage
        self.storage_lock = lock_type()

    def getMaxSupportedResourceSize(self):
        with self.storage_lock:
            return self._storage.getMaxSupportedResourceSize()

    def getRequiredWrapType(self):
        with self.storage_lock:
            return self._storage.getRequiredWrapType()

    def supportsWrapType(self, wrap_type):
        with self.storage_lock:
            return self._storage.supportsWrapType(wrap_type)

    def identifier(self):
        with self.storage_lock:
            return self._storage.identifier()

    def loadRessource(self, resource_name):
        with self.storage_lock:
            return self._storage.loadRessource(resource_name)

    def saveResource(self, resource_data, resource_hash, resource_size):
        with self.storage_lock:
            return self._storage.saveResource(resource_data, resource_hash, resource_size)

    def deleteResource(self, resource_name):
        with self.storage_lock:
            return self._storage.deleteResource(resource_name)

    def listResourceNames(self):
        with self.storage_lock:
            return self._storage.listResourceNames()

    def wipeResources(self):
        with self.storage_lock:
            return self._storage.wipeResources()


class SizableSynchronizedStorage(SizableStorageInterface, SynchronizedStorage):

    def __init__(self, storage, lock_type=Lock):
        # type: (SizableStorageInterface, Type[Lock, RLock]) -> None
        SynchronizedStorage.__init__(self, storage, lock_type)
        self._storage = cast(SizableStorageInterface, self._storage)

    def getTotalSize(self):
        return self._storage.getTotalSize()

    def getCurrentSize(self):
        return self._storage.getCurrentSize()

    def increaseCurrentSize(self, size):
        return self._storage.increaseCurrentSize(size)

    def resetCurrentSize(self):
        return self._storage.resetCurrentSize()

    def calculateFullness(self, default_total_size=None):
        return self._storage.calculateFullness(default_total_size)

    def hasFreeSize(self, required_space):
        return self._storage.hasFreeSize(required_space)
