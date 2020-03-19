import tempfile
import unittest

from ImageSaverLib4.Storage.FileSystemStorage import FileSystemStorage2
from .test_basicStorage import TestBasicStorage


class TestFileSystemStorage(TestBasicStorage):

    def acquireStorage(self):
        self.tmp_dir_context = tempfile.TemporaryDirectory()
        self.tmp_dir_context.__enter__()
        print(self.tmp_dir_context.name)
        return FileSystemStorage2(self.tmp_dir_context.name)

    def releaseStorage(self):
        self.tmp_dir_context.__exit__(None, None, None)

    def test_saveResource(self):
        super(TestFileSystemStorage, self).test_saveResource()

    def test_loadResource(self):
        super(TestFileSystemStorage, self).test_loadResource()

    def test_listResourceNames(self):
        super(TestFileSystemStorage, self).test_listResourceNames()

    def test_deleteResource(self):
        super(TestFileSystemStorage, self).test_deleteResource()

    def test_wipeResources(self):
        super(TestFileSystemStorage, self).test_wipeResources()


if __name__ == '__main__':
    unittest.main()
