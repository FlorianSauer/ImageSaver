import hashlib
import os
from typing import Optional, Set, Callable, cast

import cachetools

from ImageSaverLib4.MetaDB.Errors import NotExistingException
from ImageSaverLib4.MetaDB.MetaDB import MetaDBInterface
from ImageSaverLib4.MetaDB.Types.Resource import ResourceName, ResourceSize, ResourceHash
from ImageSaverLib4.Storage.FileSystemStorage import FileSystemStorage2
from ImageSaverLib4.Storage.StorageInterface import StorageInterface, SizableStorageInterface
from .LCMeta.LCMetaInterface import LCMetaInterface
from .LCMeta.ResourceAlias import ResourceNameAlias
from .LCMeta.db_inits import makeSQLiteMeta, makeSQLiteRamMeta


class _CallbackCache(cachetools.LFUCache):

    def __init__(self, maxsize, getsizeof=None):
        super().__init__(maxsize, getsizeof)
        self.on_delete = None  # type: Optional[Callable[[ResourceName], None]]

    def __delitem__(self, key, cache_delitem=cachetools.Cache.__delitem__):
        super().__delitem__(key, cache_delitem)
        if self.on_delete:
            self.on_delete(key)


class LocalCache(StorageInterface):
    def __init__(self, meta, storage, cache_size=50, cache_dir='~/.isl/.isl_cache', ram_cache_meta=False, debug=False):
        # type: (MetaDBInterface, StorageInterface, int, str, bool, bool) -> None
        super().__init__(debug)
        self._cache_workdir = os.path.abspath(os.path.normpath(os.path.expanduser(cache_dir)))
        self._cache_storage_workdir = os.path.join(self._cache_workdir, 'storage')
        os.makedirs(self._cache_storage_workdir, exist_ok=True)
        self._cache_meta_workdir = os.path.join(self._cache_workdir, 'cache_meta')
        os.makedirs(self._cache_meta_workdir, exist_ok=True)
        self._cache_meta_path = os.path.join(self._cache_meta_workdir, 'cache_meta.sqlite')
        self._storage = storage
        self._meta = meta
        self._local_storage = FileSystemStorage2(self._cache_storage_workdir, debug=debug)
        if ram_cache_meta:
            self._cache_meta = makeSQLiteRamMeta(echo=False)
        else:
            self._cache_meta = makeSQLiteMeta(self._cache_meta_path, echo=False)
        self._cache_size = cache_size
        self._cache = _CallbackCache(self._cache_size)
        self._cache.on_delete = lambda key: self._on_delete(key)
        self._resource_names = None  # type: Optional[Set[ResourceName]]
        unreferenced_resources = set(self._local_storage.listResourceNames()).difference(
            set((a for _, a in self._cache_meta.getAllResourceNamesWithAliases())))
        for r in unreferenced_resources:
            self._local_storage.deleteResource(r)
        for r, a in self._cache_meta.getAllResourceNamesWithAliases():
            self._cache[r] = a

    def closeCacheMeta(self):
        self._cache_meta.close()

    def supportsWrapType(self, wrap_type):
        return self._storage.supportsWrapType(wrap_type)



    def getMaxSupportedResourceSize(self):
        return self._storage.getMaxSupportedResourceSize()

    def getRequiredWrapType(self):
        return self._storage.getRequiredWrapType()

    def identifier(self):
        return self._storage.identifier()

    def _on_delete(self, resource_name):
        if self._cache_meta.hasAliasForResourceName(resource_name):
            alias = self._cache_meta.getAliasOfResourceName(resource_name)
            self._local_storage.deleteResource(alias)
            self._cache_meta.removeAliasOfResourceName(resource_name)

    def loadRessource(self, resource_name):
        try:
            alias = self._cache[resource_name]  # raises KeyError
            data = self._local_storage.loadRessource(alias)
            resource_hash = ResourceHash(hashlib.sha256(data).digest())
            try:
                meta_resource_hash = self._meta.getResourceByResourceName(resource_name).resource_hash
            except NotExistingException:
                meta_resource_hash = b''
            if resource_hash != meta_resource_hash:
                self._cache.pop(resource_name)
                self._cache_meta.removeAliasOfResourceName(resource_name)
                self._local_storage.deleteResource(alias)
                raise KeyError
        except KeyError:
            data = self._storage.loadRessource(resource_name)
            resource_hash = ResourceHash(hashlib.sha256(data).digest())
            alias = ResourceNameAlias(self._local_storage.saveResource(data, resource_hash, ResourceSize(len(data))))
            self._cache[resource_name] = alias
            self._cache_meta.addAlias(resource_name, alias, resource_hash)
        return data

    def saveResource(self, resource_data, resource_hash, resource_size):
        resource_name = self._storage.saveResource(resource_data, resource_hash, resource_size)
        alias = ResourceNameAlias(self._local_storage.saveResource(resource_data, resource_hash, resource_size))
        self._cache[resource_name] = alias
        self._cache_meta.addAlias(resource_name, alias, resource_hash)
        return resource_name

    def deleteResource(self, resource_name):
        try:
            if self._cache_meta.hasAliasForResourceName(resource_name):
                alias = self._cache_meta.getAliasOfResourceName(resource_name)
                self._local_storage.deleteResource(alias)
                self._cache_meta.removeAliasOfResourceName(resource_name)
                self._cache.pop(resource_name)
            else:
                alias = self._cache.pop(resource_name)
                self._local_storage.deleteResource(alias)
        except KeyError:
            pass
        self._storage.deleteResource(resource_name)

    def listResourceNames(self):
        return self._storage.listResourceNames()

    def wipeResources(self):
        self._storage.wipeResources()


class SizableLocalCache(SizableStorageInterface, LocalCache):

    def __init__(self, meta, storage, cache_size=50, cache_dir='~/.isl/.isl_cache', ram_cache_meta=False, debug=False):
        # type: (MetaDBInterface, SizableStorageInterface, int, str, bool, bool) -> None
        LocalCache.__init__(self, meta, storage, cache_size, cache_dir, ram_cache_meta, debug)
        self._storage = cast(SizableStorageInterface, storage)

    def getTotalSize(self):
        return self._storage.getTotalSize()

    def getCurrentSize(self):
        return self._storage.getCurrentSize()

    def increaseCurrentSize(self, size):
        self._storage.increaseCurrentSize(size)

    def resetCurrentSize(self):
        self._storage.resetCurrentSize()

    def calculateFullness(self, default_total_size=None):
        return self._storage.calculateFullness(default_total_size)

    def hasFreeSize(self, required_space):
        return self._storage.hasFreeSize(required_space)