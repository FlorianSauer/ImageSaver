import unittest

from ImageSaverLib4.Storage.Cache.RamCache import RamStorageCache
from ImageSaverLib4.Storage.RamStorage import RamStorage
from .test_basicStorage import TestBasicStorage


class TestCacheRam(TestBasicStorage):
    def acquireStorage(self):
        return RamStorageCache(RamStorage(), debug=True)

    def releaseStorage(self):
        pass

    def test_saveResource(self):
        super(TestCacheRam, self).test_saveResource()

    def test_loadResource(self):
        super(TestCacheRam, self).test_loadResource()

    def test_listResourceNames(self):
        super(TestCacheRam, self).test_listResourceNames()

    def test_deleteResource(self):
        super(TestCacheRam, self).test_deleteResource()

    def test_wipeResources(self):
        super(TestCacheRam, self).test_wipeResources()


if __name__ == '__main__':
    unittest.main()
