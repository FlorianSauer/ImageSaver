import shutil
import tempfile
import unittest

from ImageSaverLib4.Storage.Cache.LocalCache import LocalCache
from ImageSaverLib4.Storage.RamStorage import RamStorage
from .test_basicStorage import TestBasicStorage


class TestCacheLocal(TestBasicStorage):
    def acquireStorage(self):
        self.tmp_dir_context = tempfile.TemporaryDirectory()
        self.tmp_dir_context.__enter__()
        print(self.tmp_dir_context.name)
        self.cache = LocalCache(self.getMeta(), RamStorage(), cache_dir=self.tmp_dir_context.name, ram_cache_meta=True,
                                debug=True)
        return self.cache

    def releaseStorage(self):
        # input('input')
        try:
            self.cache.closeCacheMeta()
        finally:
            self.cache = None
            try:
                self.tmp_dir_context.__exit__(None, None, None)
            finally:
                # noinspection PyBroadException
                try:
                    shutil.rmtree(self.tmp_dir_context.name)
                except Exception:
                    pass

    def test_saveResource(self):
        super(TestCacheLocal, self).test_saveResource()

    def test_loadResource(self):
        super(TestCacheLocal, self).test_loadResource()

    def test_listResourceNames(self):
        super(TestCacheLocal, self).test_listResourceNames()

    def test_deleteResource(self):
        super(TestCacheLocal, self).test_deleteResource()

    def test_wipeResources(self):
        super(TestCacheLocal, self).test_wipeResources()


if __name__ == '__main__':
    unittest.main()
