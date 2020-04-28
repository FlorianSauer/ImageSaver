from typing import Optional, Set, cast

import cachetools

from ImageSaverLib.MetaDB.Types.Resource import ResourceName
from ImageSaverLib.Storage.StorageInterface import StorageInterface, SizableStorageInterface


class RamStorageCache(StorageInterface):

    def __init__(self, storage, cache_size=5, debug=False):
        # type: (StorageInterface, int, bool) -> None
        super().__init__(debug)
        self._storage = storage
        self._cache_size = cache_size
        self._cache = cachetools.LFUCache(cache_size)
        self._resource_names = None  # type: Optional[Set[ResourceName]]

    def supportsWrapType(self, wrap_type):
        return self._storage.supportsWrapType(wrap_type)

    def getMaxSupportedResourceSize(self):
        return self._storage.getMaxSupportedResourceSize()

    def getRequiredWrapType(self):
        return self._storage.getRequiredWrapType()

    def identifier(self):
        return self._storage.identifier()

    def loadRessource(self, resource_name):
        assert self._cache.currsize <= self._cache_size
        try:
            data = self._cache.get(resource_name)
            if data is None:
                raise KeyError
            self.debugPrint("loaded", resource_name, "from RAM cache")
            return data
        except KeyError:
            data = self._storage.loadRessource(resource_name)
            self.debugPrint("loaded", resource_name, "from storage", self._storage.__class__)
            assert data is not None
            self._cache[resource_name] = data
            return data

    def saveResource(self, resource_data, resource_hash, resource_size):
        assert self._cache.currsize <= self._cache_size
        resource_name = self._storage.saveResource(resource_data, resource_hash, resource_size)
        assert resource_data is not None
        self._cache[resource_name] = resource_data
        if self._resource_names:
            self._resource_names.add(resource_name)
        return resource_name

    def deleteResource(self, resource_name):
        self._storage.deleteResource(resource_name)
        try:
            self._cache.pop(resource_name)
        except KeyError:
            pass
        if self._resource_names:
            # self._resource_names = set(self._storage.listResourceNames())
            try:
                self._resource_names.remove(resource_name)
            except KeyError:
                pass

    def listResourceNames(self):
        self._resource_names = set(self._storage.listResourceNames())
        return list(self._resource_names)

    def wipeResources(self):
        self._storage.wipeResources()
        self._cache.clear()
        if self._resource_names is not None:
            self._resource_names.clear()


class SizableRamStorageCache(SizableStorageInterface, RamStorageCache):

    def __init__(self, storage, cache_size=5, debug=False):
        # type: (SizableStorageInterface, int, bool) -> None
        RamStorageCache.__init__(self, storage, cache_size, debug)
        self._storage = cast(SizableStorageInterface, storage)

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
