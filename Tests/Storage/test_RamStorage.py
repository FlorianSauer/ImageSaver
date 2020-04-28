import unittest

from ImageSaverLib.Storage.RamStorage import RamStorage
from .test_basicStorage import TestBasicStorage


class TestRamStorage(TestBasicStorage):

    def acquireStorage(self):
        return RamStorage()

    def releaseStorage(self):
        pass

    def test_saveResource(self):
        super(TestRamStorage, self).test_saveResource()

    def test_loadResource(self):
        super(TestRamStorage, self).test_loadResource()

    def test_listResourceNames(self):
        super(TestRamStorage, self).test_listResourceNames()

    def test_deleteResource(self):
        super(TestRamStorage, self).test_deleteResource()

    def test_wipeResources(self):
        super(TestRamStorage, self).test_wipeResources()


if __name__ == '__main__':
    unittest.main()
