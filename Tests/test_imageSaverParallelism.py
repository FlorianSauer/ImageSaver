import time
from threading import Lock, Thread, Event
from typing import cast, Generator, Tuple
from unittest import TestCase

from ImageSaverLib.MetaDB.Types.Fragment import FragmentID
from ImageSaverLib.Storage.RamStorage import RamStorage
from ImageSaverLib.ImageSaverLib import ImageSaver
from ImageSaverLib.MetaDB import db_inits
from ImageSaverLib.Encapsulation.Wrappers.Types import PassThroughWrapper

VERBOSE_SAVE_SERVICE = False


class NotifyRamStorage(RamStorage):

    def __init__(self):
        super(NotifyRamStorage, self).__init__()
        self.remove_lock = Lock()

    def continueRemove(self, sleep=0.1):
        self.remove_lock.release()
        time.sleep(sleep)

    def deleteResource(self, resource_name):
        self.remove_lock.acquire()
        try:
            super().deleteResource(resource_name)
        finally:
            self.remove_lock.release()
            self.remove_lock.acquire()

    def wipeResources(self):
        super().wipeResources()

    def saveResource(self, resource_data, resource_hash, resource_size):
        return super().saveResource(resource_data, resource_hash, resource_size)

    def loadRessource(self, resource_name):
        return super().loadRessource(resource_name)

    def listResourceNames(self):
        return super().listResourceNames()


class SlowedDownRamStorage(RamStorage):

    def __init__(self, slowdown=0.2):
        super(SlowedDownRamStorage, self).__init__()
        self.slowdown = slowdown

    def deleteResource(self, resource_name):
        time.sleep(self.slowdown)
        super().deleteResource(resource_name)

    def wipeResources(self):
        time.sleep(self.slowdown)
        super().wipeResources()

    def saveResource(self, resource_data, resource_hash, resource_size):
        time.sleep(self.slowdown)
        return super().saveResource(resource_data, resource_hash, resource_size)

    def loadRessource(self, resource_name):
        time.sleep(self.slowdown)
        return super().loadRessource(resource_name)

    def listResourceNames(self):
        time.sleep(self.slowdown)
        return super().listResourceNames()


class TestAbstractSaveServiceParallelism(TestCase):
    raise NotImplementedError('todo: update from v2 to v4')
    # noinspection PyMethodMayBeStatic
    def makeSaveService(self):
        # type: () -> ImageSaver
        storage = RamStorage()
        meta = db_inits.sqliteRAM()
        saver = ImageSaver(meta, storage)
        saver.changeFragmentSize((2.0, 'B'))
        saver.wrap_type = PassThroughWrapper
        saver.compress_type = PassThroughCompresser
        return saver

    def makeNotifySaveService(self):
        # type: () -> Tuple[ImageSaver, NotifyRamStorage]
        storage = NotifyRamStorage()
        meta = db_inits.sqliteRAM()
        saver = ImageSaver(meta, storage)
        saver.changeFragmentSize((2.0, 'B'))
        saver.wrap_type = PassThroughWrapper
        saver.compress_type = PassThroughCompresser
        return saver, storage

    def makeSlowSaveService(self):
        # type: () -> Tuple[ImageSaver, SlowedDownRamStorage]
        storage = SlowedDownRamStorage()
        meta = db_inits.sqliteRAM()
        saver = ImageSaver(meta, storage)
        saver.changeFragmentSize((2.0, 'B'))
        saver.wrap_type = PassThroughWrapper
        saver.compress_type = PassThroughCompresser
        return saver, storage

    def consumeGenerator(self, gen):
        # type: (Generator) -> None
        for _ in gen:
            pass

    def getAccessCount(self, service, fid):
        # type: (ImageSaver, int) -> int
        return service.reserved_fragments.accessCountForValue(service.meta.getFragmentByID(cast(FragmentID, fid)).fragment_payload_hash)

    def getFDHForFID(self, service, kw, fid):
        return service._metadatastorage.getFragmentDictByCompoundName(cast(CompoundName, kw))[cast(FragmentID, fid)]

    def test_loadSingleData(self):
        service = self.makeSaveService()
        service.saveBytes(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadCompoundBytes('kw1'))

        chunk_gen = service.loadCompound('kw1')
        # wrong, only during calculation of needed fragments
        # self.assertEqual(1, len(service._compoundMappings_locked_compounds))  
        # self.assertTrue('kw1' in service._compoundMappings_locked_compounds)
        self.assertEqual(0, len(service._compoundMappings_access_manager.managedValues()))
        self.assertEqual(6, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'he', next(chunk_gen))
        self.assertEqual(5, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'll', next(chunk_gen))
        self.assertEqual(4, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'o ', next(chunk_gen))
        self.assertEqual(3, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'wo', next(chunk_gen))
        self.assertEqual(2, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'rl', next(chunk_gen))
        self.assertEqual(1, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'd', next(chunk_gen))
        self.assertEqual(0, len(service._fragment_access_manager.managedValues()))
        self.assertFalse('kw1' in service._compoundMappings_access_manager)
        self.assertEqual(0, len(service._compoundMappings_access_manager.managedValues()))

    def test_loadSingleDataParallelWithRemove(self):
        service = self.makeSaveService()
        service.saveDataB(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadDataB('kw1'))

        chunk_gen1 = service.loadData('kw1')
        chunk_gen2 = service.loadData('kw1')

        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)

        self.assertTrue(self.getFDHForFID(service, 'kw1', 0) in service._fragment_access_manager)
        self.assertEqual(2, self.getAccessCount(service, 'kw1', 0))
        self.assertEqual(6, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'he', next(chunk_gen1))
        self.assertTrue(self.getFDHForFID(service, 'kw1', 0) in service._fragment_access_manager)
        self.assertEqual(1, self.getAccessCount(service, 'kw1', 0))
        self.assertEqual(6, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'he', next(chunk_gen2))
        self.assertFalse(self.getFDHForFID(service, 'kw1', 0) in service._fragment_access_manager)
        self.assertEqual(5, len(service._fragment_access_manager.managedValues()))

        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)

        self.assertTrue(self.getFDHForFID(service, 'kw1', 1) in service._fragment_access_manager)
        self.assertEqual(2, self.getAccessCount(service, 'kw1', 1))
        self.assertEqual(5, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'll', next(chunk_gen1))
        self.assertTrue(self.getFDHForFID(service, 'kw1', 1) in service._fragment_access_manager)
        self.assertEqual(1, self.getAccessCount(service, 'kw1', 1))
        self.assertEqual(5, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'll', next(chunk_gen2))
        self.assertFalse(self.getFDHForFID(service, 'kw1', 1) in service._fragment_access_manager)
        self.assertEqual(4, len(service._fragment_access_manager.managedValues()))

        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)

        self.assertTrue(self.getFDHForFID(service, 'kw1', 2) in service._fragment_access_manager)
        self.assertEqual(2, self.getAccessCount(service, 'kw1', 2))
        self.assertEqual(4, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'o ', next(chunk_gen1))
        self.assertTrue(self.getFDHForFID(service, 'kw1', 2) in service._fragment_access_manager)
        self.assertEqual(1, self.getAccessCount(service, 'kw1', 2))
        self.assertEqual(4, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'o ', next(chunk_gen2))
        self.assertFalse(self.getFDHForFID(service, 'kw1', 2) in service._fragment_access_manager)
        self.assertEqual(3, len(service._fragment_access_manager.managedValues()))

        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)

        self.assertTrue(self.getFDHForFID(service, 'kw1', 3) in service._fragment_access_manager)
        self.assertEqual(2, self.getAccessCount(service, 'kw1', 3))
        self.assertEqual(3, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'wo', next(chunk_gen1))
        self.assertTrue(self.getFDHForFID(service, 'kw1', 3) in service._fragment_access_manager)
        self.assertEqual(1, self.getAccessCount(service, 'kw1', 3))
        self.assertEqual(3, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'wo', next(chunk_gen2))
        self.assertFalse(self.getFDHForFID(service, 'kw1', 3) in service._fragment_access_manager)
        self.assertEqual(2, len(service._fragment_access_manager.managedValues()))

        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)

        self.assertTrue(self.getFDHForFID(service, 'kw1', 4) in service._fragment_access_manager)
        self.assertEqual(2, self.getAccessCount(service, 'kw1', 4))
        self.assertEqual(2, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'rl', next(chunk_gen1))
        self.assertTrue(self.getFDHForFID(service, 'kw1', 4) in service._fragment_access_manager)
        self.assertEqual(1, self.getAccessCount(service, 'kw1', 4))
        self.assertEqual(2, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'rl', next(chunk_gen2))
        self.assertFalse(self.getFDHForFID(service, 'kw1', 4) in service._fragment_access_manager)
        self.assertEqual(1, len(service._fragment_access_manager.managedValues()))

        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)

        self.assertTrue(self.getFDHForFID(service, 'kw1', 5) in service._fragment_access_manager)
        self.assertEqual(2, self.getAccessCount(service, 'kw1', 5))
        self.assertEqual(1, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'd', next(chunk_gen1))
        self.assertTrue(self.getFDHForFID(service, 'kw1', 5) in service._fragment_access_manager)
        self.assertEqual(1, self.getAccessCount(service, 'kw1', 5))
        self.assertEqual(1, len(service._fragment_access_manager.managedValues()))
        self.assertEqual(b'd', next(chunk_gen2))
        self.assertFalse(self.getFDHForFID(service, 'kw1', 5) in service._fragment_access_manager)
        self.assertEqual(0, len(service._fragment_access_manager.managedValues()))

        # self.consumeGenerator(chunk_gen1)
        # self.consumeGenerator(chunk_gen2)
        service.removeData('kw1', blocking=False)
        self.assertNotIn('kw1', service.listCompoundNames())

        # self.consumeGenerator(chunk_gen1)
        # self.consumeGenerator(chunk_gen2)
        self.assertEqual(0, len(service._compoundMappings_access_manager.managedValues()))
        self.assertEqual(0, len(service._fragment_access_manager.managedValues()))
        # self.assertIn('kw1', service.listCompoundNames())
        # service.removeData('kw1')
        # self.assertNotIn('kw1', service.listCompoundNames())

    def test_removeSingleDataParallelWithLoad(self):
        service = self.makeNotifySaveService()
        service.saveDataB(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadDataB('kw1'))
        t = Thread(target=service.removeData, args=('kw1',))  # he
        t.daemon = True
        t.start()
        # time.sleep(1)
        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)
        self.assertRaises(NonBlockingException, service.loadData, 'kw1', blocking=False)
        self.assertIn('kw1', service.listCompoundNames())
        service.continueRemove()  # ll
        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)
        self.assertRaises(NonBlockingException, service.loadData, 'kw1', blocking=False)
        self.assertIn('kw1', service.listCompoundNames())
        service.continueRemove()  # o
        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)
        self.assertRaises(NonBlockingException, service.loadData, 'kw1', blocking=False)
        self.assertIn('kw1', service.listCompoundNames())
        service.continueRemove()  # wo
        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)
        self.assertRaises(NonBlockingException, service.loadData, 'kw1', blocking=False)
        self.assertIn('kw1', service.listCompoundNames())
        service.continueRemove()  # rl
        self.assertRaises(NonBlockingException, service.removeData, 'kw1', blocking=False)
        self.assertRaises(TimeoutException, service.removeData, 'kw1', timeout=0.1)
        self.assertRaises(NonBlockingException, service.loadData, 'kw1', blocking=False)
        self.assertIn('kw1', service.listCompoundNames())
        service.continueRemove()  # d
        self.assertRaises(CompoundMissingError, service.removeData, 'kw1')
        self.assertRaises(CompoundMissingError, service.loadData, 'kw1', blocking=False)
        self.assertNotIn('kw1', service.listCompoundNames())
        t.join()

    def test_concurrentLoadRemove(self):
        notifyer = Event()
        assert not notifyer.is_set()
        service = self.makeSlowSaveService()
        service.slowdown = 0
        service.saveDataB(bytes(b'hello world'), 'kw1')
        self.assertEqual(bytes(b'hello world'), service.loadDataB('kw1'))
        print_lock = Lock()
        service.slowdown = 0.2

        def _load_thread():
            notifyer.wait()
            try:
                list(service.loadData('kw1', blocking=False))
                with print_lock:
                    print("loaded kw1")
            except NonBlockingException:
                with print_lock:
                    print("load thread was too slow")
            except CompoundMissingError:
                with print_lock:
                    print("Compound missing, load thread was too slow")

        def _remove_thread():
            notifyer.wait()
            try:
                service.removeData('kw1', blocking=False)
                with print_lock:
                    print("removed kw1")
            except NonBlockingException:
                with print_lock:
                    print("remove thread was too slow")

        load_thread = Thread(target=_load_thread)
        remove_thread = Thread(target=_remove_thread)

        load_thread.daemon = True
        remove_thread.daemon = True

        load_thread.start()
        remove_thread.start()

        notifyer.set()

        load_thread.join()
        remove_thread.join()
