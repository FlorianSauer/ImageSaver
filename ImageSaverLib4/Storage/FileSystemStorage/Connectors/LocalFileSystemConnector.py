import os
import shutil

from . import FileSystemInterface
from ..Errors import SaveError, LoadError, DeleteError


class LocalFileSystemConnector(FileSystemInterface):
    @classmethod
    def identifier(cls):
        return 'local'

    @classmethod
    def os_makedirs(cls, path):
        os.makedirs(path, exist_ok=True)

    @classmethod
    def os_rmdir(cls, path):
        if os.path.exists(path):
            shutil.rmtree(path)

    @classmethod
    def saveFile(cls, data, path):
        try:
            with open(path, 'wb') as f:
                f.write(data)
            return True
        except OSError as e:
            raise SaveError("Unable to save data to " + repr(path) + ': ' + repr(e))

    @classmethod
    def loadFile(cls, path):
        try:
            with open(path, 'rb') as f:
                return f.read()
        except OSError as e:
            raise LoadError("Unable to load data from " + repr(path) + ': ' + repr(e))

    @classmethod
    def deleteFile(cls, path):
        # type: (str) -> None
        # return
        try:
            os.remove(path)
        except OSError as e:
            raise DeleteError("Unable to delete " + repr(path) + ': ' + repr(e))

    @classmethod
    def os_walk(cls, path):
        return os.walk(path)

    @classmethod
    def fileSize(cls, path):
        return os.path.getsize(path)

    @classmethod
    def remaining(cls, path):
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        usage = shutil.disk_usage(path)
        orig_total = usage.total
        orig_used = usage.used
        saver_used = cls.treeSize(path)
        other_used = orig_used - saver_used
        return orig_total - other_used

