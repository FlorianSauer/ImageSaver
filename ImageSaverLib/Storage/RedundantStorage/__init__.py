import hashlib
import json
import os
import random
from typing import Dict, List, Optional, Union, Callable, Iterator, Set

from ImageSaverLib.Encapsulation.Wrappers.Types import PassThroughWrapper
from ImageSaverLib.MetaDB.MetaDB import MetaDBInterface
from ImageSaverLib.MetaDB.Types.Resource import ResourceName, Resource, ResourceSize
from ImageSaverLib.Storage.RedundantStorage.RSMeta.ManagedStorage import StorageIdentifier
from ImageSaverLib.Storage.RedundantStorage.RSMeta.RSMetaInterface import RSMetaInterface
from ImageSaverLib.Storage.RedundantStorage.RSMeta.ResourceAlias import ResourceNameAlias
from .RSMeta.db_inits import makeSQLiteMeta
from ..Errors import NotFoundError, ManagementError, StorageError
from ..StorageInterface import StorageInterface, SizableStorageInterface


class RedundantStorage(StorageInterface):
    def __init__(self, policy, redundancy, *storages, debug=False, meta_dir='~/.isl/.pool', meta=None):
        # type: (int, int, *Union[StorageInterface, SizableStorageInterface], bool, str, Optional[RSMetaInterface]) -> None
        super().__init__(debug)
        required_wrap_types = set((s.getRequiredWrapType() for s in storages))
        if PassThroughWrapper.get_wrapper_type() in required_wrap_types:
            required_wrap_types.remove(PassThroughWrapper.get_wrapper_type())
        if len(required_wrap_types) > 1:
            raise ManagementError("inconsistent required wrap types: " + repr(required_wrap_types))
        elif len(required_wrap_types) == 1:
            self.required_wrap_type = required_wrap_types.pop()
        else:
            assert len(required_wrap_types) == 0
        self.max_resource_size = min((s.getMaxSupportedResourceSize() for s in storages))
        self.policy = policy
        self.redundancy = redundancy
        self._storages = {self._makeStorageIdent(s): s for s in
                          storages}  # type: Dict[str, Union[StorageInterface, SizableStorageInterface]]
        self._exluded_storages = set()  # type: Set[Union[StorageInterface, SizableStorageInterface]]
        # meta
        self._meta_workdir = os.path.abspath(os.path.normpath(os.path.expanduser(meta_dir)))
        self._meta_path = os.path.join(self._meta_workdir, 'pool_meta.sqlite')
        if meta:
            self._meta = meta
        else:
            self._meta = makeSQLiteMeta(self._meta_path, echo=False)

    def identifier(self):
        return '; '.join((s.identifier() for s in self._storages.values()))

    def loadRessource(self, resource_name):
        alias = self._meta.getAliasOfResourceName(resource_name)
        storage_hashes__resource_names = json.loads(alias)  # type: Dict[str, ResourceName]
        matching_storages = set(self._storages.keys()).intersection(set(storage_hashes__resource_names.keys()))
        if len(matching_storages) == 0:
            raise NotFoundError("Unable to download Resource, No storage matches.")
        storage = random.choice(list(matching_storages))
        return self._storages[storage].loadRessource(storage_hashes__resource_names[storage])

    def saveResource(self, resource_data, resource_hash, resource_size):
        names = {}
        storage_list = self.getPolicyStorageList(resource_size, self.redundancy)
        try:
            for storage in storage_list:
                ident = self._makeStorageIdent(storage)
                try:
                    self.debugPrint('uploading', resource_size, 'bytes to storage', ident)
                    resource_name = storage.saveResource(resource_data, resource_hash, resource_size)
                    names[ident] = resource_name
                except StorageError as e:
                    self.debugPrint('upload to', ident, 'failed:', repr(e))
                    storage_list.getStorageReplacement(storage)
        except RedundancyError:
            for ident, resource_name in names.items():
                self.debugPrint('deleting resource', resource_name, 'from', ident, 'because of a redundancy error')
                self._storages[ident].deleteResource(resource_name)
            raise
        resource_name = self._addResourceAlias(names)
        return resource_name

    def _addResourceAlias(self, storage_ident_resource_name_mapping):
        # type: (Dict[str, ResourceName]) -> ResourceName
        alias = json.dumps(storage_ident_resource_name_mapping, sort_keys=True)
        resource_name = hashlib.sha256(alias.encode('utf-8')).hexdigest()
        self._meta.addAlias(resource_name, alias)
        self._meta.makeMultipleManagedStorages(
            [StorageIdentifier(i) for i in storage_ident_resource_name_mapping.keys()])
        return resource_name

    def _addResourceNameToExistingAliased(self, existing_resource_name, added_storage_ident, added_resource_name):
        alias = self._meta.getAliasOfResourceName(existing_resource_name)
        storage_ident_resource_name_mapping = json.loads(alias)  # type: Dict[str, ResourceName]
        storage_ident_resource_name_mapping[added_storage_ident] = added_resource_name
        alias_json = ResourceNameAlias(json.dumps(storage_ident_resource_name_mapping, sort_keys=True))
        self._meta.renameAlias(existing_resource_name,
                               alias_json)
        self._meta.makeMultipleManagedStorages(
            [StorageIdentifier(i) for i in storage_ident_resource_name_mapping.keys()])
        assert alias_json == self._meta.getAliasOfResourceName(existing_resource_name)

    def deleteResource(self, resource_name):
        alias = self._meta.getAliasOfResourceName(resource_name)
        storage_hashes__resource_names = json.loads(alias)  # type: Dict[str, ResourceName]
        matching_storages = set(self._storages.keys()).intersection(set(storage_hashes__resource_names.keys()))
        if len(matching_storages) == 0:
            raise NotFoundError("Unable to delete Resource, No storage matches.")
        self._meta.removeAliasOfResourceName(resource_name)
        for storage_ident in matching_storages:
            self._storages[storage_ident].deleteResource(storage_hashes__resource_names[storage_ident])

    def listResourceNames(self):
        return list(self._meta.getAllResourceNames())

    def wipeResources(self):
        for storage in self._storages.values():
            storage.wipeResources()
        for resource_name in list(self._meta.getAllResourceNames()):
            self._meta.removeAliasOfResourceName(resource_name)

    PERCENTAGE = 1
    SIZE = 2

    def addStorage(self, storage):
        # type: (Union[StorageInterface, SizableStorageInterface]) -> None
        key = self._makeStorageIdent(storage)
        if key not in self._storages:
            self._storages[key] = storage
            # self._meta.makeManagedStorage(key)

    def listManagedStorages(self):
        """
        return a list of storages, which are managed currently or previously by the storage pool.
        useful to check if a storage (which was previously used without a pool) should get newly poolified or not.
        """
        return self._meta.listManagedStorages()

    def managesStorage(self, storage):
        return self._meta.hasManagedStorage(self._makeStorageIdent(storage))

    def _minfree_cutting(self, sorted_list, min_free=None):
        # type: (List[SizableStorageInterface], int) -> List[SizableStorageInterface]
        if min_free:
            # print([(s, s.getTotalSize()) for s in sorted_list if s.getTotalSize() > 0])
            return [s for s in sorted_list if s.hasFreeSize(min_free)]
        return sorted_list

    def _get_sizable_storages(self):
        # type: () -> List[SizableStorageInterface]
        return [s for s in self._storages.values() if
                isinstance(s, SizableStorageInterface) and s not in self._exluded_storages]

    def _get_non_sizable_storages(self):
        # type: () -> List[StorageInterface]
        return [s for s in self._storages.values() if
                not isinstance(s, SizableStorageInterface) and s not in self._exluded_storages]

    def getPercentageSorted(self, redundancy=1, min_free=None):
        # type: (int, Optional[int]) -> StorageResultList
        """
        first get size of biggest storage (excluding infinite storages)
        calculate fullness, treat infinite storages as if their size is the same as the biggest found storage
        :param redundancy:
        :param min_free:
        :return:
        """
        sorted_list = self._get_sizable_storages()
        max_storage_size = max((s.getTotalSize() for s in sorted_list))
        sorted_list = sorted(sorted_list, key=lambda s: s.calculateFullness(max_storage_size))
        sorted_list = self._minfree_cutting(sorted_list, min_free)
        sorted_list += self._get_non_sizable_storages()
        return StorageResultList(sorted_list, redundancy)
        # return self._redundancy_cutting(sorted_list, redundancy)

    def getSizeSorted(self, redundancy=1, min_free=None):
        # type: (int, Optional[int]) -> StorageResultList
        sorted_list = self._get_sizable_storages()
        sorted_list = sorted(sorted_list, key=lambda s: s.getCurrentSize())
        sorted_list = self._minfree_cutting(sorted_list, min_free)
        sorted_list += self._get_non_sizable_storages()
        return StorageResultList(sorted_list, redundancy)
        # return self._redundancy_cutting(sorted_list, redundancy)

    def getStorageTotalSizeSorted(self, redundancy=1, min_free=None):
        # type: (int, Optional[int]) -> StorageResultList[Union[StorageInterface, SizableStorageInterface]]
        sorted_list = self._get_sizable_storages()
        sorted_list = sorted(sorted_list, key=lambda s: s.getTotalSize())
        sorted_list = self._minfree_cutting(sorted_list, min_free)
        sorted_list += self._get_non_sizable_storages()
        return StorageResultList(sorted_list, redundancy)
        # return self._redundancy_cutting(sorted_list, redundancy)

    def getAllStorages(self):
        # type: () -> List[Union[StorageInterface, SizableStorageInterface]]
        return self._get_sizable_storages() + self._get_non_sizable_storages()

    def getPolicyStorageList(self, byte_count, redundancy):
        # type: (int, int) -> StorageResultList
        if self.policy == self.PERCENTAGE:
            return self.getPercentageSorted(redundancy, byte_count)
        elif self.policy == self.SIZE:
            return self.getSizeSorted(redundancy, byte_count)
        raise Exception

    def poolifySingleStorage(self, storage, meta):
        # type: (StorageInterface, MetaDBInterface) -> None
        """
        wraps a given storage with this pool.
        useful for poolifying a single storage if the storage already contains data.
        This operation does NOT upload or download resources from or to storages.
        """
        old_new_resource_names = []
        storage_ident = self._makeStorageIdent(storage)
        for i, old_resource_name in enumerate(storage.listResourceNames()):
            new_resource_name = self._addResourceAlias({storage_ident: old_resource_name})
            old_new_resource_names.append((old_resource_name, new_resource_name))
        meta.massRenameResource(old_new_resource_names, skip_unknown=True)

    def integrateStorageIntoPool(self, storage):
        # type: (StorageInterface) -> None
        """
        downloads all resources from a given storage and distributes it to the other registered storages.
        If the given storage is part of the pool, then this storage is ignored during data distribution.
        in this case the redundancy is reduced by 1.

        :return:
        """
        if self.managesStorage(storage):
            # exception raised, because we would have to search all aliases and therefore all json dict values if
            # a resource (and resourcename) from the given storage already exists in the pool and if other
            # storages also already contain this resource. Resources should/must be unique on each storage
            raise Exception('storage already managed, can not import')
        src_storage_ident = self._makeStorageIdent(storage)
        self._exluded_storages.add(storage)
        redundancy_reduced = src_storage_ident in self._storages
        if redundancy_reduced:
            self.redundancy -= 1
        try:
            for src_resource_name in storage.listResourceNames():
                src_resource_data = storage.loadRessource(src_resource_name)
                new_resource_name = self.saveResource(src_resource_data,
                                                      Resource.makeResourceHash(src_resource_data),
                                                      ResourceSize(len(src_resource_data)))
                self._addResourceNameToExistingAliased(new_resource_name, src_storage_ident, src_resource_name)
        finally:
            self._exluded_storages.remove(storage)
            if redundancy_reduced:
                self.redundancy += 1

    def separateSingleStorageFromPool(self, ):
        raise NotImplementedError

    def _makeStorageIdent(self, storage):
        return hashlib.md5(storage.identifier().encode('utf-8')).hexdigest()

    # def upload(self, upload_type, byte_count, redundancy=1):
    #     # type: (int, int, int) -> None
    #     if upload_type == self.PERCENTAGE:
    #         storage_list = self.getPercentageSorted(redundancy, byte_count)
    #     elif upload_type == self.SIZE:
    #         storage_list = self.getSizeSorted(redundancy, byte_count)
    #     else:
    #         raise Exception
    #     for storage in storage_list:
    #         storage.upload(byte_count)


class RedundancyError(StorageError):
    pass


class StorageResultList(Iterator):

    def __init__(self, storages, redundancy):
        # type: (List[StorageInterface], int) -> None
        self.storages = list(storages)
        self.remaining_storages = list(storages)
        self.replaced_storages = set()
        self.redundancy = redundancy
        if len(self.remaining_storages) < redundancy:
            raise RedundancyError('not enough storages to fulfill redundancy')

    def __next__(self):
        # type: () -> StorageInterface
        # red = 3
        # all = 10, rest = 7
        if ((len(self.storages) - len(self.replaced_storages)) - len(self.remaining_storages) >= self.redundancy and self.redundancy != -1) or (
                self.redundancy == -1 and len(self.remaining_storages) == 0):
            raise StopIteration
        # if len(self.remaining_storages) < self.redundancy or len(self.remaining_storages) == 0:
        if len(self.remaining_storages) == 0:
            raise RedundancyError('not enough storages to fulfill redundancy')
        return self.remaining_storages.pop(0)

    def getStorageReplacement(self, storage):
        # type: (StorageInterface) -> None
        self.replaced_storages.add(storage)

    # def cutOffRedundancyList(self, redundancy=-1):
    #     if redundancy == -1:
    #         return self.remaining_storages
    #     if len(self.remaining_storages) < redundancy:
    #         raise Exception('not enough storages to fulfill redundancy')
    #     redundancy_list = self.remaining_storages[:redundancy]
    #     self.remaining_storages = self.remaining_storages[redundancy:]
    #     return redundancy_list
    #
    # def getNextReplacementStorage(self):
    #     return self.remaining_storages.pop(0)
