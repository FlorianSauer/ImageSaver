import hashlib
from typing import Optional, BinaryIO, Iterator, AnyStr, Iterable, List, Tuple

from ImageSaverLib.Encapsulation import WrappingType, CompressionType, AutoWrapper, AutoCompressor, encapsulate
from ImageSaverLib.Errors import CompoundAlreadyExistsException
from ImageSaverLib.FragmentCache import FragmentCache
from ImageSaverLib.Helpers import split_bytes
from ImageSaverLib.Helpers.ControlledAccess.AccessManager import AccessManager
from ImageSaverLib.Helpers.ControlledAccess.Context.AccessContext import AccessContext
from ImageSaverLib.Helpers.ControlledAccess.Context.ExclusiveAccessContext import ExclusiveAccessContext
from ImageSaverLib.Helpers.ControlledAccess.Reserver.MassReserver import MassReserver
from ImageSaverLib.Helpers.ControlledAccess.Reserver.ParallelMassReserver import ParallelMassReserver
from ImageSaverLib.MetaDB.Errors import NotExistingException
from ImageSaverLib.MetaDB.MetaDB import MetaDBInterface
from ImageSaverLib.MetaDB.Types.Compound import (CompoundName, Compound, CompoundType, CompoundSize,
                                                 CompoundCompressionType, CompoundWrappingType, CompoundHash,
                                                 CompoundVersion)
from ImageSaverLib.MetaDB.Types.CompoundFragmentMapping import SequenceIndex
from ImageSaverLib.MetaDB.Types.Fragment import FragmentHash, FragmentPayloadSize, Fragment
from ImageSaverLib.PendingObjectsController import PendingObjectsController


def openWritableCompound(meta, fragment_cache, compound_am, fragment_am, fragment_size, wrapper, compresser, wrap_type,
                         compress_type, pending_objects, name, compound_type, overwrite=False, compound=None,
                         blocking=True, timeout=None):
    # type: (MetaDBInterface, FragmentCache, AccessManager[Tuple[CompoundName, CompoundVersion]], AccessManager[FragmentHash], int, AutoWrapper, AutoCompressor, WrappingType, CompressionType, PendingObjectsController, CompoundName, CompoundType, bool, Optional[Compound], bool, Optional[float]) -> WritableCompound
    if compound:
        assert compound.compound_name == name
    compound_reserver = ExclusiveAccessContext(compound_am,
                                               (name, CompoundVersion(None)),
                                               blocking=blocking, timeout=timeout)
    fragment_reserver = ParallelMassReserver(fragment_am, blocking=blocking, timeout=timeout)
    with compound_reserver:
        with meta:
            if compound and compound.compound_id is not None:  # compound has id from db
                meta_has_compound = True
            else:
                if pending_objects.hasCompoundWithName(name):  # compound has no id and is in pending
                    compound = pending_objects.getPendingCompoundWithName(name)
                    meta_has_compound = True
                    compound_is_pending = True
                else:
                    try:
                        compound = meta.getCompoundByName(name)
                        meta_has_compound = True
                    except NotExistingException:
                        meta_has_compound = False
                        compound = None
                    compound_is_pending = False
            if meta_has_compound and not overwrite:
                raise CompoundAlreadyExistsException(
                    "compound already exists (maybe different payload), not allowed to overwrite")
            if meta_has_compound and not compound_is_pending:
                assert compound.compound_id is not None
                fragment_hashes = list(meta.getFragmentHashesNeededForCompound(compound.compound_id))
                fragment_reserver.reserveAll(*fragment_hashes)
            elif meta_has_compound and compound_is_pending:
                fragment_hashes = list(pending_objects.getFragmentHashesNeededForCompound(compound.compound_hash))
                fragment_reserver.reserveAll(*fragment_hashes)

        w_c = WritableCompound(meta, fragment_cache, compound_reserver, fragment_reserver, fragment_size, wrapper,
                               compresser, wrap_type, compress_type, pending_objects, name, compound_type)
        return w_c


class WritableCompound(BinaryIO):

    def __init__(self, meta, fragment_cache, compound_reserver, fragment_reserver, fragment_size, wrapper, compresser,
                 wrap_type, compress_type, pending_objects, name, compound_type):
        # type: (MetaDBInterface, FragmentCache, AccessContext[Tuple[CompoundName, CompoundVersion]], MassReserver[FragmentHash], int, AutoWrapper, AutoCompressor, WrappingType, CompressionType, PendingObjectsController, CompoundName, CompoundType) -> None
        pass
        self.__debug = False
        # must contain meta, fragmentcache, reserved fragments+compounds
        # region meta, fragmentcache, other backreporting and shared controllers
        self._meta = meta
        self._fragment_cache = fragment_cache
        self._compound_reserver = compound_reserver
        self._fragment_reserver = fragment_reserver
        self._fragment_size = fragment_size
        self._wrapper = wrapper
        self._compresser = compresser
        self._wrap_type = wrap_type
        self._compress_type = compress_type
        self._pending_objects = pending_objects
        self._name = name
        self._compound_type = compound_type
        # endregion

        # region buffers and indexes
        self._stream_hash = hashlib.sha256()
        self._stream_size = CompoundSize(0)
        self._fragment_data_buffer = bytes()
        self._fragment_data_buffer_len = 0
        self._payload_index = SequenceIndex(0)
        self._fragment_payload_index = []  # type: List[Tuple[Fragment, SequenceIndex]]
        self._pending_fragments = []
        self._pending_compound = None
        self._skip_fragment_hashes = set()
        # endregion

        # region BinaryIO object helper fields
        self._closed = False
        self.__reserved = False
        # endregion

        # update skippable fragment hashes with values from fragment reserver
        # which might have fragment hashes from an already existing compound
        self._skip_fragment_hashes.update(set(self._fragment_reserver.listReservedValues()))
        if self.__debug:
            print('WritableCompound __init__', self._name)
        self.__enter__()
        self._reserve()

    def _reserve(self):
        if not self.__reserved:
            if self.__debug:
                print('WritableCompound _reserve', self._name)
            # noinspection PyBroadException
            try:
                self.__reserved = True
                self._compound_reserver.__enter__()
                self._fragment_reserver.__enter__()
            except Exception:
                self._unreserve()

    def _unreserve(self):
        if self.__reserved:
            if self.__debug:
                print('WritableCompound _unreserve', self._name)
            self.__reserved = False
            try:
                self._fragment_reserver.__exit__(None, None, None)
            finally:
                self._compound_reserver.__exit__(None, None, None)

    def __enter__(self):
        if self.__debug:
            print('WritableCompound __enter__', self._name)
        self._reserve()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__debug:
            print('WritableCompound __exit__', exc_type, exc_val, exc_tb)
        try:
            if exc_tb:
                pass
            else:
                self._close()
        finally:
            self._unreserve()
        return

    def write(self, chunk):
        # type: (bytes) -> None
        if self.__debug:
            print('WritableCompound write', self._name, len(chunk))

        # add chunk to internal buffer, flush complete fragments if possible
        if chunk:
            self._stream_hash.update(chunk)
            self._stream_size += len(chunk)
            # fill internal buffer of fragment_size size
            self._fragment_data_buffer += chunk
            self._fragment_data_buffer_len += len(chunk)
        if self._fragment_data_buffer_len >= self._fragment_size or (not chunk and self._fragment_data_buffer_len > 0):
            self._flush_full_fragments()

    def close(self):
        if self.__debug:
            print('WritableCompound close', self._name)
        self._close()

    def _close(self):
        try:
            if self.__debug:
                print('WritableCompound _close', self._name)
            # flush internal buffer completely
            self.flush()
            # process compound metadata
            stream_hash = CompoundHash(self._stream_hash.digest())
            compound = Compound(self._name, self._compound_type, stream_hash,
                                self._stream_size,
                                CompoundWrappingType(self._wrap_type),
                                CompoundCompressionType(self._compress_type))
            self._pending_compound = compound

            assert compound.compound_size == self._stream_size
            if self._pending_compound:
                self._pending_objects.addCompound(self._pending_compound, self._pending_fragments,
                                                  self._fragment_payload_index)

            # map fragment ids and indexes to compound
            self._fragment_cache.flushMeta()
            # disable fragment cache flushing
            # self._fragment_cache.flush()
            self._closed = True
        finally:
            self._unreserve()

    def flush(self):
        if self.__debug:
            print('WritableCompound flush')

        pass
        # while data in buffer, flush one fragment, until buffer is completely empty
        self._flush()

    def _flush(self):
        if self.__debug:
            print('WritableCompound _flush')

        while self._fragment_data_buffer_len > 0:
            self._flush_one_fragment()

    def _flush_full_fragments(self):
        if self.__debug:
            print('WritableCompound _flush_full_fragments')

        # while data in buffer, flush only full fragments, keep remaining data in buffer
        while self._one_full_fragment_flushable():
            self._flush_one_fragment()

    def _one_full_fragment_flushable(self):
        if self.__debug:
            print('WritableCompound _one_full_fragment_flushable')

        # return bool, if one full fragment is flushable
        return self._fragment_data_buffer_len >= self._fragment_size

    def _flush_one_fragment(self):
        if self.__debug:
            print('WritableCompound _flush_one_fragment')

        pass
        # reduce internal buffer by only one fragment, if buffer is not sufficiently filled, flush remaining
        if self._fragment_data_buffer_len <= self._fragment_size:
            fragment_data = self._fragment_data_buffer
            self._fragment_data_buffer = bytes()
            self._fragment_data_buffer_len = 0
        else:
            fragment_data, self._fragment_data_buffer = split_bytes(self._fragment_data_buffer, self._fragment_size)
            self._fragment_data_buffer_len -= self._fragment_size

        # remember fragment id and payload index
        fragment_payload_size = FragmentPayloadSize(len(fragment_data))
        fragment_data = encapsulate(self._compresser, self._wrapper, self._compress_type, self._wrap_type,
                                    fragment_data)
        fragment_hash = FragmentHash(hashlib.sha256(fragment_data).digest())
        self._fragment_reserver.reserveOne(fragment_hash)
        fragment = self._fragment_cache.addFragmentData(fragment_data, fragment_hash, fragment_payload_size)
        self._pending_fragments.append(fragment)
        self._fragment_payload_index.append((fragment, self._payload_index))
        self._payload_index += 1

    def closed(self) -> bool:
        return self._closed

    def fileno(self) -> int:
        raise NotImplementedError("Method 'fileno' is not supported on a WritableCompound object")

    def isatty(self) -> bool:
        raise NotImplementedError("Method 'isatty' is not supported on a WritableCompound object")

    def read(self, n: int = ...) -> AnyStr:
        raise NotImplementedError("Method 'read' is not supported on a WritableCompound object")

    def readable(self) -> bool:
        return False

    def readline(self, limit: int = ...) -> AnyStr:
        raise NotImplementedError("Method 'readline' is not supported on a WritableCompound object")

    def readlines(self, hint: int = ...) -> List[AnyStr]:
        raise NotImplementedError("Method 'readlines' is not supported on a WritableCompound object")

    def seek(self, offset: int, whence: int = ...) -> int:
        raise NotImplementedError("Method 'seek' is not supported on a WritableCompound object")

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        raise NotImplementedError("Method 'tell' is not supported on a WritableCompound object")

    def truncate(self, size: Optional[int] = ...) -> int:
        raise NotImplementedError("Method 'truncate' is not supported on a WritableCompound object")

    def writable(self) -> bool:
        return not self._closed

    def writelines(self, lines: Iterable[AnyStr]) -> None:
        raise NotImplementedError("Method 'writelines' is not supported on a WritableCompound object")

    def __next__(self) -> AnyStr:
        raise NotImplementedError("Method '__next__' is not supported on a WritableCompound object")

    def __iter__(self) -> Iterator[AnyStr]:
        raise NotImplementedError("Method '__iter__' is not supported on a WritableCompound object")

    def mode(self) -> str:
        return 'wb'

    def name(self) -> str:
        return self._name
