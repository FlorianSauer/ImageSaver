import hashlib
import os
from unittest import TestCase

from ImageSaverLib4.FragmentCache import FragmentCache
from ImageSaverLib4.MetaDB.Types.Fragment import FragmentSize, FragmentPayloadSize
from ImageSaverLib4.MetaDB.Types.Resource import ResourceSize
from ImageSaverLib4.MetaDB.db_inits import sqliteRAM, sqliteFile
from ImageSaverLib4.PendingObjectsController import PendingObjectsController
from ImageSaverLib4.Storage.RamStorage import RamStorage
from ImageSaverLib4.Storage.Cache.RamCache import RamStorageCache
from ImageSaverLib4.Encapsulation import makeWrappingType, makeCompressingType
from ImageSaverLib4.Encapsulation.Wrappers.Types import *
from ImageSaverLib4.Encapsulation.Compressors.Types import *
from ImageSaverLib4.Storage.VoidStorage import VoidStorage


class TestFragmentCache(TestCase):
    TEST_ITERATIONS=250000

    def test_addFragment(self):
        self.fail()
        # cache_meta = sqliteFile('R:/tmp.sqlite', recreate=True)
        meta = sqliteRAM()
        storage = VoidStorage()
        storage_cache = RamStorageCache(storage)
        # Todo: update fragment cache init
        fragment_cache = FragmentCache(meta, storage_cache, 100,
                                       makeWrappingType(PassThroughWrapper),
                                       makeCompressingType(PassThroughCompressor),
                                       ResourceSize(1000), PendingObjectsController(), )
        fragment_cache.policy = fragment_cache.POLICY_PASS
        with fragment_cache:
            input('start')
            for index in range(self.TEST_ITERATIONS):
                fragment_data = os.urandom(100)
                fragment = meta.makeFragment(hashlib.sha256(fragment_data).digest(), FragmentSize(100), FragmentPayloadSize(100))
                fragment_cache.addFragment(fragment_data, fragment)
                if index/self.TEST_ITERATIONS >= 0.2 or index >= 250000:
                    input('break')
        input('done')

    def test_loadFragment(self):
        self.fail()

    def test_flush(self):
        self.fail()

    def test__flush(self):
        self.fail()

    def test__upload(self):
        self.fail()

    def test__download(self):
        self.fail()

    def test__download_fragments_of_resource(self):
        self.fail()

    def test__upload_and_map_fragments(self):
        self.fail()

    def test__flush_percentage_filled(self):
        self.fail()

    def test__flush_resource_appending(self):
        self.fail()
