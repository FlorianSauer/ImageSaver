from abc import abstractmethod

from typing import Tuple

from ImageSaverLib4.Helpers.SizedGenerator import SizedGenerator
from ImageSaverLib4.MetaDB.Types.Resource import ResourceName, ResourceHash
from .ResourceAlias import ResourceNameAlias


class LCMetaInterface(object):
    def __init__(self):
        pass

    @abstractmethod
    def addAlias(self, resource_name, alias, resource_hash):
        # type: (ResourceName, ResourceNameAlias, ResourceHash) -> None
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
    def getResourceHashForAlias(self, alias):
        # type: (ResourceNameAlias) -> ResourceHash
        pass

    @abstractmethod
    def close(self):
        # type: () -> None
        pass