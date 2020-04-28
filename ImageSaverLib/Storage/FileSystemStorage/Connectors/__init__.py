import posixpath
from abc import abstractmethod
from typing import Iterable, List, Tuple


class FileSystemInterface(object):

    @abstractmethod
    def identifier(self):
        # type: () -> str
        pass

    @classmethod
    @abstractmethod
    def os_makedirs(cls, path):
        # type: (str) -> None
        pass

    @classmethod
    @abstractmethod
    def os_rmdir(cls, path):
        # type: (str) -> None
        pass

    @classmethod
    @abstractmethod
    def saveFile(cls, data, path):
        # type: (bytes, str) -> bool
        """
        :raises SaveError
        """
        pass

    @classmethod
    @abstractmethod
    def loadFile(cls, path):
        # type: (str) -> bytes
        """
        :raises LoadError
        """
        pass

    @classmethod
    @abstractmethod
    def deleteFile(cls, path):
        # type: (str) -> None
        pass

    @classmethod
    @abstractmethod
    def os_walk(cls, path):
        # type: (str) -> Iterable[Tuple[str, List[str], List[str]]]
        pass

    @classmethod
    def path_join(cls, path, *paths):
        # type: (str, *str) -> str
        return posixpath.join(path, *paths)

    @classmethod
    def treeSize(cls, path):
        # type: (str) -> int
        size = 0
        for dirname, _, files in cls.os_walk(path):
            for f in files:
                size += cls.fileSize(cls.path_join(dirname, f))
        return size

    @classmethod
    @abstractmethod
    def fileSize(cls, path):
        # type: (str) -> int
        pass
