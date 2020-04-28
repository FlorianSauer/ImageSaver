from abc import abstractmethod

from typing import Tuple, List, Dict

from ImageSaverLib.Helpers.SizedGenerator import SizedGenerator
from ImageSaverLib.MetaDB.Types.Resource import ResourceName
from ImageSaverLib.Storage.RedundantStorage.RSMeta.ManagedStorage import StorageIdentifier, ManagedStorage
from .ResourceAlias import ResourceNameAlias


class RSMetaInterface(object):
    def __init__(self):
        pass

    @abstractmethod
    def addAlias(self, resource_name, alias):
        # type: (ResourceName, ResourceNameAlias) -> None
        pass

    @abstractmethod
    def renameAlias(self, resource_name, alias):
        # type: (ResourceName, ResourceNameAlias) -> None
        pass

    @abstractmethod
    def getAliasOfResourceName(self, resource_name):
        # type: (ResourceName) -> ResourceNameAlias
        pass

    @abstractmethod
    def hasAliasForResourceName(self, resource_name):
        # type: (ResourceName) -> bool
        pass

    @abstractmethod
    def removeAliasOfResourceName(self, resource_name):
        # type: (ResourceName) -> None
        pass

    @abstractmethod
    def getAllResourceNames(self):
        # type: () -> SizedGenerator[ResourceName]
        pass

    @abstractmethod
    def getAllResourceNamesWithAliases(self):
        # type: () -> SizedGenerator[Tuple[ResourceName, ResourceNameAlias]]
        pass

    @abstractmethod
    def close(self):
        # type: () -> None
        pass

    @abstractmethod
    def makeManagedStorage(self, storage_ident):
        # type: (StorageIdentifier) -> ManagedStorage
        pass

    @abstractmethod
    def makeMultipleManagedStorages(self, storage_ident_list):
        # type: (List[StorageIdentifier]) -> Dict[StorageIdentifier, ManagedStorage]
        pass

    @abstractmethod
    def listManagedStorages(self):
        # type: () -> SizedGenerator[ManagedStorage]
        pass

    def hasManagedStorage(self, storage_ident):
        # type: (StorageIdentifier) -> bool
        pass
