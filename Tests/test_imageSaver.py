from typing import cast
from unittest import TestCase

from tqdm import tqdm
import humanfriendly

from ImageSaverLib4.Encapsulation import makeWrappingType, makeCompressingType
from ImageSaverLib4.Encapsulation.Wrappers.Types import *
from ImageSaverLib4.Encapsulation.Compressors.Types import *
from ImageSaverLib4.Errors import *
from ImageSaverLib4.ImageSaverLib import ImageSaver
from ImageSaverLib4.MetaDB.db_inits import sqliteRAM
from ImageSaverLib4.Storage.Errors import NotFoundError
from ImageSaverLib4.Storage.RamStorage import RamStorage

VERBOSE_SAVE_SERVICE = False


class TestAbstractSaveService(TestCase):
    # noinspection PyMethodMayBeStatic
    def makeSaveService(self):
        # type: () -> ImageSaver
        meta = sqliteRAM()
        storage = RamStorage()
        service = ImageSaver(meta, storage, 2, 4)
        service.wrap_type = makeWrappingType(PassThroughWrapper)
        service.compress_type = makeCompressingType(PassThroughCompressor)
        return service

    def test_randomSave(self):
        service = self.makeSaveService()
        for index in tqdm(range(20000)):
            service.saveBytes(str(index).encode('ascii'), str(index))
        # input('done')

    def test_saveLoadSingleData(self):
        service = self.makeSaveService()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadDataTwice(self):
        service = self.makeSaveService()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        service.flush()
        self.assertRaises(CompoundAlreadyExistsException, service.saveBytes, bytes(b'hello world'), 'kw1')

    def test_saveLoadDataFragmentMissing(self):
        # self.skipTest("todo")
        service = self.makeSaveService()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        service.flush()
        service.storage.deleteResource(service.storage.listResourceNames()[-1])
        # print(bytes().join(service.loadCompound('kw1')))
        # bytes().join(service.loadCompound('kw1'))
        self.assertRaises(NotFoundError, service.loadCompoundBytes, 'kw1')

    def test_saveLoadDataFragmentInconsistent(self):
        # only testing with ram-storage
        service = self.makeSaveService()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        service.flush()
        if type(service.storage) != RamStorage:
            self.skipTest('this test only works with RAM-Storage')
        name = cast(RamStorage, service.storage).storage.list()[0]
        data = cast(RamStorage, service.storage).storage.load(name)
        data = bytearray(data)
        data.reverse()
        data = bytes(data)
        cast(RamStorage, service.storage).storage.add(name, data)
        self.assertRaises(ResourceManipulatedException, service.loadCompoundBytes, 'kw1')

    def test_saveLoadPassthroughWrapper(self):
        service = self.makeSaveService()
        service.wrap_type = PassThroughWrapper.get_wrapper_type()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadSizeChecksumWrapper(self):
        service = self.makeSaveService()
        service.wrap_type = SizeChecksumWrapper.get_wrapper_type()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadPassThroughWrapper(self):
        service = self.makeSaveService()
        service.wrap_type = PassThroughWrapper.get_wrapper_type()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadSVGWrapper(self):
        service = self.makeSaveService()
        service.wrap_type = SVGWrapper.get_wrapper_type()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadSizeChecksumSVGWrapper(self):
        service = self.makeSaveService()
        service.wrap_type = SizeChecksumWrapper.get_wrapper_type() + '-' + SVGWrapper.get_wrapper_type()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadPassThroughCompresser(self):
        service = self.makeSaveService()
        service.compress_type = PassThroughCompressor.get_compressor_type()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadBZ2Compresser(self):
        service = self.makeSaveService()
        service.compress_type = BZ2Compressor.get_compressor_type()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))
        service = self.makeSaveService()
        service.saveBytes(bytes(b'hello world'), 'kw1', compress_type=BZ2Compressor.get_compressor_type())
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadLZMACompresser(self):
        service = self.makeSaveService()
        service.compress_type = LZMACompressor.get_compressor_type()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))
        service = self.makeSaveService()
        service.saveBytes(bytes(b'hello world'), 'kw1', compress_type=LZMACompressor.get_compressor_type())
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadZLibCompresser(self):
        service = self.makeSaveService()
        service.compress_type = ZLibCompressor.get_compressor_type()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))
        service = self.makeSaveService()
        service.saveBytes(bytes(b'hello world'), 'kw1', compress_type=ZLibCompressor.get_compressor_type())
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

    def test_saveLoadFragmentSizeIncrease(self):
        service = self.makeSaveService()
        service.saveBytes(bytes(b'helloworld'), 'kw1')
        service.flush()
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        self.assertEqual(5, service.getTotalFragmentCount())
        self.assertEqual(3, service.getTotalResourceCount())
        self.assertEqual(bytes(b'helloworld'), service.loadCompoundBytes('kw1'))

        service.changeFragmentSize(humanfriendly.parse_size('4 B'))
        service.saveBytes(bytes(b'helloworld'), 'kw1', overwrite=True)
        service.flush()
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        # ending 'ld' is equal, 'hell' and 'owor' gets added
        self.assertEqual(5 + 2, service.getTotalFragmentCount())
        self.assertEqual(3, service.getTotalResourceCount())
        self.assertEqual(bytes(b'helloworld'), service.loadCompoundBytes('kw1'))

    def test_saveLoadFragmentSizeDecrease(self):
        service = self.makeSaveService()
        service.changeFragmentSize(humanfriendly.parse_size('4 B'))
        service.saveBytes(bytes(b'helloworld'), 'kw1')
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        self.assertEqual(3, service.getTotalFragmentCount())
        self.assertEqual(3, service.getTotalResourceCount())
        service.changeFragmentSize(humanfriendly.parse_size('2 B'))
        self.assertEqual(bytes(b'helloworld'), service.loadCompoundBytes('kw1'))
        service.saveBytes(bytes(b'helloworld'), 'kw1', overwrite=True)
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        # ending 'ld' is equal, 'hell' and 'owor' gets added
        self.assertEqual(3 + 4, service.getTotalFragmentCount())
        self.assertEqual(3 + 4, service.getTotalResourceCount())
        self.assertEqual(bytes(b'helloworld'), service.loadCompoundBytes('kw1'))

    def test_garbageCollect(self):
        service = self.makeSaveService()
        service.changeFragmentSize(humanfriendly.parse_size('2 B'))
        service.saveBytes(b'hello world', 'kw1', overwrite=True)
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        self.assertEqual(6, service.getTotalFragmentCount())
        self.assertEqual(6, service.getTotalResourceCount())
        service.saveBytes(b'hello world', 'kw1', overwrite=True)
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        self.assertEqual(6, service.getTotalFragmentCount())
        self.assertEqual(6, service.getTotalResourceCount())
        service.saveBytes(b'hello world2', 'kw1', overwrite=True)
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(2, service.getUniqueCompoundCount())
        self.assertEqual(7, service.getTotalFragmentCount())
        self.assertEqual(7, service.getTotalResourceCount())
        service.saveBytes(b'hello world2', 'kw2', overwrite=True)
        self.assertEqual(2, service.getTotalCompoundCount())
        self.assertEqual(2, service.getUniqueCompoundCount())
        self.assertEqual(7, service.getTotalFragmentCount())
        self.assertEqual(7, service.getTotalResourceCount())
        service.collectGarbage()
        self.assertEqual(2, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        self.assertEqual(6, service.getTotalFragmentCount())
        self.assertEqual(6, service.getTotalResourceCount())

        ###

        service = self.makeSaveService()
        service.changeFragmentSize(humanfriendly.parse_size('2 B'))
        service.saveBytes(b'hello world', 'kw1', overwrite=True)
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        self.assertEqual(6, service.getTotalFragmentCount())
        self.assertEqual(6, service.getTotalResourceCount())
        service.saveBytes(b'hello world2', 'kw1', overwrite=True)
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(2, service.getUniqueCompoundCount())
        self.assertEqual(7, service.getTotalFragmentCount())
        self.assertEqual(7, service.getTotalResourceCount())
        service.collectGarbage()
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        self.assertEqual(6, service.getTotalFragmentCount())
        self.assertEqual(6, service.getTotalResourceCount())

        ###

        service = self.makeSaveService()
        service.changeFragmentSize(humanfriendly.parse_size('2 B'))
        for i in range(100):
            service.saveBytes(b'hello world-' + str(i).encode('ascii'), 'kw1', overwrite=True)
            self.assertEqual(1, service.getTotalCompoundCount())
            self.assertEqual(1 + i, service.getUniqueCompoundCount())
            self.assertEqual(7 + i, service.getTotalFragmentCount())
            self.assertEqual(7 + i, service.getTotalResourceCount())
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(100, service.getUniqueCompoundCount())
        self.assertEqual(7 + 99, service.getTotalFragmentCount())
        self.assertEqual(7 + 99, service.getTotalResourceCount())

        service.collectGarbage()
        self.assertEqual(1, service.getTotalCompoundCount())
        self.assertEqual(1, service.getUniqueCompoundCount())
        self.assertEqual(7, service.getTotalFragmentCount())
        self.assertEqual(7, service.getTotalResourceCount())
