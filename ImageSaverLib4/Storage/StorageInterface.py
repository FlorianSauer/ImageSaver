from abc import ABC, abstractmethod
from typing import List, Optional, NewType

from ImageSaverLib4.Encapsulation import WrappingType
from ImageSaverLib4.Encapsulation.Wrappers.Types import PassThroughWrapper
# noinspection PyUnresolvedReferences
from .Errors import (DownloadError, NotFoundError, UploadError, DeleteError, ListError, WipeError)
from ..Encapsulation.Compressors.BaseCompressor import BaseCompressor
from ..Encapsulation.Wrappers.BaseWrapper import BaseWrapper
from ..MetaDB.Types.Resource import ResourceName, ResourceHash, ResourceSize

StorageSize = NewType('StorageSize', int)


class StorageInterface(ABC):
    supported_comresserclasses = []  # type: List[BaseCompressor]
    supported_wrapperclasses = []  # type: List[BaseWrapper]

    DEFAULT_MAX_RESOURCE_SIZE = ResourceSize(10000000)  # 10 MB
    max_resource_size = DEFAULT_MAX_RESOURCE_SIZE

    DEFAULT_WRAP_TYPE = PassThroughWrapper.get_wrapper_type()
    required_wrap_type = DEFAULT_WRAP_TYPE

    def __init__(self, debug=False, wrap_type=None, max_resource_size=None):
        # type: (bool, Optional[WrappingType], Optional[ResourceSize]) -> None
        self.__debug = debug
        if wrap_type:
            self.required_wrap_type = wrap_type
        if max_resource_size:
            self.max_resource_size = max_resource_size

    def getMaxSupportedResourceSize(self):
        return self.max_resource_size

    def getRequiredWrapType(self):
        return self.required_wrap_type

    def supportsWrapType(self, wrap_type):
        # type: (WrappingType) -> bool
        return wrap_type.endswith(self.getRequiredWrapType())

    @property
    def debug(self):
        # type: () -> bool
        return self.__debug

    @debug.setter
    def debug(self, value):
        # type: (bool) -> None
        self.__debug = value

    def debugPrint(self, *args):
        if self.__debug:
            s = []
            for a in args:
                if type(a) in (float, int):
                    s.append(str(a))
                elif type(a) is str:
                    s.append(a)
                else:
                    s.append(repr(a))
            print(' '.join(s))

    @abstractmethod
    def identifier(self):
        # type: () -> str
        pass

    @classmethod
    @abstractmethod
    def loadRessource(cls, resource_name):
        # type: (ResourceName) -> bytes
        """
        :raises DownloadError:
        :raises NotFoundError:
        """
        pass

    @classmethod
    @abstractmethod
    def saveResource(cls, resource_data, resource_hash, resource_size):
        # type: (bytes, ResourceHash, ResourceSize) -> ResourceName
        """
        :raises UploadError:
        """
        pass

    @classmethod
    @abstractmethod
    def deleteResource(cls, resource_name):
        # type: (ResourceName) -> None
        """
        :raises DeleteError:
        """
        pass

    @classmethod
    @abstractmethod
    def listResourceNames(cls):
        # type: () -> List[ResourceName]
        """
        :raises ListError:
        """
        pass

    @classmethod
    @abstractmethod
    def wipeResources(cls):
        # type: () -> None
        """
        :raises WipeError:
        """
        pass


class SizableStorageInterface(StorageInterface, ABC):
    INFINITE_STORAGE_SIZE = StorageSize(-1)
    DEFAULT_MAX_STORAGE_SIZE = INFINITE_STORAGE_SIZE  # set to none, to calculate the max size if needed

    @abstractmethod
    def getTotalSize(self):
        # type: () -> StorageSize
        pass

    @abstractmethod
    def getCurrentSize(self):
        # type: () -> int
        pass

    @abstractmethod
    def increaseCurrentSize(self, size):
        # type: (int) -> None
        pass

    @abstractmethod
    def resetCurrentSize(self):
        # type: () -> None
        pass

    @abstractmethod
    def calculateFullness(self, default_total_size=None):
        # type: (Optional[int]) -> float
        pass

    @abstractmethod
    def hasFreeSize(self, required_space):
        # type: (int) -> bool
        pass


class AbstractSizableStorageInterface(SizableStorageInterface, ABC):
    def __init__(self, debug=False, wrap_type=None, max_resource_size=None, max_storage_size=None):
        # type: (bool, Optional[WrappingType], Optional[ResourceSize], Optional[StorageSize]) -> None
        super(SizableStorageInterface, self).__init__(debug, wrap_type, max_resource_size)
        self._total_size = None  # type: Optional[StorageSize]
        if max_storage_size:
            self._default_total_size = max_storage_size
        else:
            self._default_total_size = self.DEFAULT_MAX_STORAGE_SIZE
        self._current_size = None  # type: Optional[int]

    def getTotalSize(self):
        # type: () -> StorageSize
        if self._total_size is None:
            self._total_size = StorageSize(self._calculateTotalSize())
        return self._total_size

    def getCurrentSize(self):
        if self._current_size is None:
            self._current_size = self._calculateCurrentSize()
        return self._current_size

    @abstractmethod
    def _calculateCurrentSize(self):
        # type: () -> int
        pass

    def _calculateTotalSize(self):
        # type: () -> int
        if self._default_total_size is None:
            return self.INFINITE_STORAGE_SIZE
        return self._default_total_size

    def increaseCurrentSize(self, size):
        # type: (int) -> None
        if self._current_size is None:
            return
        self._current_size += size

    def resetCurrentSize(self):
        self._current_size = None

    def calculateFullness(self, default_total_size=None):
        # type: (Optional[int]) -> float
        if self.getTotalSize() == self.INFINITE_STORAGE_SIZE:
            if default_total_size is not None:
                return self.getCurrentSize() / default_total_size
            return 0.0
        return self.getCurrentSize() / self.getTotalSize()

    def hasFreeSize(self, required_space):
        if self.getTotalSize() == self.INFINITE_STORAGE_SIZE:
            return True
        else:
            return self.getCurrentSize() + required_space <= self.getTotalSize()
