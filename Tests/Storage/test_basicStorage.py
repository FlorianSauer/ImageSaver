import hashlib
import os
import unittest
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import cast

import humanfriendly

from ImageSaverLib.Encapsulation import AutoWrapper
from ImageSaverLib.MetaDB.MetaDB import MetaDBInterface
from ImageSaverLib.MetaDB.Types.Resource import ResourceSize, ResourceHash, ResourcePayloadSize, \
    ResourceCompressionType
from ImageSaverLib.MetaDB.db_inits import sqliteRAM
from ImageSaverLib.Storage.StorageInterface import StorageInterface


class TestBasicStorage(unittest.TestCase, ABC):
    test_data_size = humanfriendly.parse_size('1 MB')
    test_upload_count = 10

    @abstractmethod
    def acquireStorage(self):
        # type: () -> StorageInterface
        pass

    @abstractmethod
    def releaseStorage(self):
        pass

    def pre_process_storage(self, resource_data):
        # type: (bytes) -> bytes
        return resource_data

    def post_process_storage(self, resource_data):
        # type: (bytes) -> bytes
        return resource_data

    @contextmanager
    def withStorage(self):
        self._meta = sqliteRAM()
        storage = self.acquireStorage()
        try:
            yield storage
        finally:
            self.releaseStorage()
            self._meta = None

    def getMeta(self):
        # type: () -> MetaDBInterface
        if self._meta is None:
            raise RuntimeError('can only get meta shortly before withStorage(): acquireStorage() call')
        return self._meta

    @abstractmethod
    def test_saveResource(self):
        self._test_saveResource()

    def _test_saveResource(self):
        auto_wrapper = AutoWrapper()
        with self.withStorage() as storage:  # type: StorageInterface
            test_data = os.urandom(storage.getMaxSupportedResourceSize())
            test_data_wrapped = auto_wrapper.wrap(test_data, storage.getRequiredWrapType())
            storage.saveResource(test_data_wrapped, hashlib.sha256(test_data_wrapped).digest(),
                                 cast(ResourceSize, len(test_data_wrapped)))

    @abstractmethod
    def test_loadResource(self):
        self._test_loadResource()

    def _test_loadResource(self):
        auto_wrapper = AutoWrapper()
        with self.withStorage() as storage:  # type: StorageInterface
            test_data = os.urandom(storage.getMaxSupportedResourceSize())
            test_data_wrapped = auto_wrapper.wrap(test_data, storage.getRequiredWrapType())
            resource_size = cast(ResourceSize, len(test_data_wrapped))
            resource_hash = cast(ResourceHash, hashlib.sha256(test_data_wrapped).digest())
            key = storage.saveResource(test_data_wrapped, resource_hash, resource_size)
            self.getMeta().makeResource(key, resource_size, cast(ResourcePayloadSize, len(test_data)), resource_hash,
                                        storage.getRequiredWrapType(), cast(ResourceCompressionType, ''))
            retrieved_test_data_wrapped = storage.loadRessource(key)
            retrieved_test_data = auto_wrapper.unwrap(retrieved_test_data_wrapped, storage.getRequiredWrapType())
            self.assertEqual(test_data, retrieved_test_data)

    @abstractmethod
    def test_listResourceNames(self):
        self._test_listResourceNames()

    def _test_listResourceNames(self):
        auto_wrapper = AutoWrapper()
        saved_keys = []
        with self.withStorage() as storage:  # type: StorageInterface
            for i in range(self.test_upload_count):
                test_data = os.urandom(storage.getMaxSupportedResourceSize())
                test_data_wrapped = auto_wrapper.wrap(test_data, storage.getRequiredWrapType())
                key = storage.saveResource(test_data_wrapped, hashlib.sha256(test_data_wrapped).digest(),
                                           cast(ResourceSize, len(test_data_wrapped)))
                saved_keys.append(key)
            self.assertSetEqual(set(saved_keys), set(storage.listResourceNames()))

    @abstractmethod
    def test_deleteResource(self):
        self._test_deleteResource()

    def _test_deleteResource(self):
        auto_wrapper = AutoWrapper()
        saved_keys = []
        with self.withStorage() as storage:  # type: StorageInterface
            for i in range(self.test_upload_count):
                test_data = os.urandom(storage.getMaxSupportedResourceSize())
                test_data_wrapped = auto_wrapper.wrap(test_data, storage.getRequiredWrapType())
                key = storage.saveResource(test_data_wrapped, hashlib.sha256(test_data_wrapped).digest(),
                                           cast(ResourceSize, len(test_data_wrapped)))
                saved_keys.append(key)
            for key in saved_keys:
                saved_keys_len = len(saved_keys)
                storage.deleteResource(key)
                saved_keys.remove(key)
                assert len(saved_keys) == saved_keys_len - 1
                self.assertSetEqual(set(saved_keys), set(storage.listResourceNames()))

    @abstractmethod
    def test_wipeResources(self):
        self._test_wipeResources()

    def _test_wipeResources(self):
        auto_wrapper = AutoWrapper()
        saved_keys = []
        with self.withStorage() as storage:  # type: StorageInterface
            for i in range(self.test_upload_count):
                test_data = os.urandom(storage.getMaxSupportedResourceSize())
                test_data_wrapped = auto_wrapper.wrap(test_data, storage.getRequiredWrapType())
                key = storage.saveResource(test_data_wrapped, hashlib.sha256(test_data_wrapped).digest(),
                                           cast(ResourceSize, len(test_data_wrapped)))
                saved_keys.append(key)
            storage.wipeResources()
            self.assertEqual(0, len(list(storage.listResourceNames())))


if __name__ == '__main__':
    unittest.main()
