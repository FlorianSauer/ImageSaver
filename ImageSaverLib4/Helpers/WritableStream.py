import hashlib
from typing import Optional, BinaryIO, Iterator, AnyStr, Iterable, List, Tuple

from ImageSaverLib4.Encapsulation import WrappingType, CompressionType, AutoWrapper, AutoCompressor, encapsulate
from ImageSaverLib4.Errors import CompoundAlreadyExistsException
from ImageSaverLib4.FragmentCache import FragmentCache
from ImageSaverLib4.Helpers import split_bytes
from ImageSaverLib4.Helpers.NotifyCounter import (AccessManager, ExclusiveAccessContext,
                                                  ParallelMassReserver, AccessContext, MassReserver)
from ImageSaverLib4.MetaDB.MetaDB import MetaDBInterface
from ImageSaverLib4.MetaDB.Types.Compound import (CompoundName, Compound, CompoundType, CompoundSize,
                                                  CompoundCompressionType, CompoundWrappingType)
from ImageSaverLib4.MetaDB.Types.CompoundFragmentMapping import SequenceIndex
from ImageSaverLib4.MetaDB.Types.Fragment import FragmentHash, FragmentPayloadSize, Fragment
from ImageSaverLib4.PendingObjectsController import PendingObjectsController

# noinspection PyUnresolvedReferences
# class _WritableStream(BinaryIO):
#
#     def fileno(self) -> int:
#         raise NotImplementedError
#
#     def isatty(self) -> bool:
#         raise NotImplementedError
#
#     def read(self, n: int = ...) -> AnyStr:
#         raise NotImplementedError
#
#     def readable(self) -> bool:
#         return False
#
#     def readline(self, limit: int = ...) -> AnyStr:
#         raise NotImplementedError
#
#     def readlines(self, hint: int = ...) -> List[AnyStr]:
#         raise NotImplementedError
#
#     def seek(self, offset: int, whence: int = ...) -> int:
#         raise NotImplementedError
#
#     def seekable(self) -> bool:
#         return False
#
#     def tell(self) -> int:
#         raise NotImplementedError
#
#     def truncate(self, size: Optional[int] = ...) -> int:
#         raise NotImplementedError
#
#     def writable(self) -> bool:
#         raise NotImplementedError
#
#     def writelines(self, lines: Iterable[AnyStr]) -> None:
#         raise NotImplementedError
#
#     def __next__(self) -> AnyStr:
#         raise NotImplementedError
#
#     def __iter__(self) -> Iterator[AnyStr]:
#         raise NotImplementedError
#
#     # def __init__(self, meta, fragment_cache, reserved_fragments, blocking, timeout):
#     #     # type: (MetaDBInterface, FragmentCache, AccessManager[FragmentHash], bool, Optional[float]) -> None
#     #     super().__init__(b'')
#     #     self._meta = meta
#     #     self._fragment_cache = fragment_cache
#     #     self._fragment_reserver = ExclusiveMassReserver(reserved_fragments, blocking=blocking, timeout=timeout)
#
#     # def write(self, b):
#     #     # type: (Union[bytes, bytearray]) -> int
#     #     return_val = super().write(b)
#
#     def __enter__(self):
#         # type: () -> WritableStream
#         return_val = super().__enter__()
#         self._meta.__enter__()
#         self._fragment_cache.__enter__()
#         self._compound_reserver.__enter__()
#         self._fragment_reserver.__enter__()
#         self._meta_has_compound = self._meta.hasCompoundWithName(self._name)
#
#         if self._meta_has_compound and not self._overwrite:
#             raise CompoundAlreadyExistsException(
#                 "compound already exists (maybe different payload), not allowed to overwrite")
#         elif self._meta_has_compound and self._overwrite:
#             self._compound = self._meta.getCompoundByName(self._name)
#             fragment_hashes = list(self._meta.getFragmentHashesNeededForCompound(self._compound.compound_id))
#             self._fragment_reserver.reserveAll(*fragment_hashes)
#             self._skip_fragment_hashes.update(fragment_hashes)
#             # if pre_calc_stream_hash:
#             #     _stream_hash = get_sha256_of_stream(stream, chunksize=read_speed)
#             #     if compound.compound_hash == _stream_hash:
#             #         # print("pre hashing worked :)")
#             #         raise CompoundAlreadyExistsException(
#             #             "compound already exists with same payload, overwrite not needed")
#         else:
#             self._compound = None
#             # compound = self.cache_meta.makeCompound(name, compound_type)
#         return return_val
#
#     def close(self) -> None:
#         stream_hash = self._stream_hash.digest()
#         if not self._compound:
#             compound = self._meta.makeCompound(self._name, self._compound_type, stream_hash, self._stream_size,
#                                                CompoundWrappingType(self._wrap_type),
#                                                CompoundCompressionType(self._compress_type))
#
#             # assert self.cache_meta.getCompoundByName(name)
#             # assert self.cache_meta.hasCompoundWithName(name)
#         else:
#             compound = self._meta.updateCompound(self._name, self._compound_type, stream_hash, self._stream_size,
#                                                  CompoundWrappingType(self._wrap_type),
#                                                  CompoundCompressionType(self._compress_type))
#         if compound.compound_size > 0:
#             self._pending_objects.addCompound(compound, self._pending_fragments)
#         # map fragment ids and indexes to compound
#         self._meta.setFragmentsMappingForCompound(compound.compound_id, self._fragment_payload_index)
#
#     def _clean_on_error(self, exc_type):
#         for c in self._pending_objects.getPendingCompounds():
#             print("removing pending compound", c.compound_name, "due to an error", exc_type)
#             self._meta.removeCompound(c.compound_id)
#             self._pending_objects.removeCompound(c)
#         print("removing", len(self._pending_objects.getPendingFragments()), "pending fragments", "due to an error",
#               exc_type)
#         self._meta.deleteFragments(self._pending_objects.getPendingFragments())
#         for f in self._pending_objects.getPendingFragments():
#             self._pending_objects.removeFragment(f)
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         try:
#             if exc_tb:
#                 self._clean_on_error(exc_type)
#             else:
#                 try:
#                     self.close()
#                 except Exception:
#                     self._clean_on_error(exc_type)
#                     raise
#         finally:
#             self._meta.__exit__(exc_type, exc_val, exc_tb)
#             self._fragment_cache.__exit__(exc_type, exc_val, exc_tb)
#             self._fragment_reserver.__exit__(exc_type, exc_val, exc_tb)
#             self._compound_reserver.__exit__(exc_type, exc_val, exc_tb)
#
#     def __init__(self, meta, fragment_cache, reserved_compounds, name, reserved_fragments, overwrite, compound_type,
#                  wrap_type, compress_type, fragment_size, compressor, wrapper):
#         # type: (MetaDBInterface, FragmentCache, AccessManager[CompoundName], CompoundName, AccessManager[FragmentHash], bool, CompoundType, WrappingType, CompressionType, FragmentSize, AutoCompressor, AutoWrapper) -> None
#         self._name = CompoundName(name)
#         self._compound_type = CompoundType(compound_type)
#         self._wrap_type = wrap_type
#         self._compress_type = compress_type
#         self._fragment_size = fragment_size
#
#         self._meta = meta
#         self._fragment_cache = fragment_cache
#         self._compresser = compressor
#         self._wrapper = wrapper
#         self._reserved_compounds = reserved_compounds
#         self._compound_reserver = ExclusiveAccessContext(self._reserved_compounds, name, blocking=blocking,
#                                                          timeout=timeout)
#         self._fragment_reserver = ExclusiveMassReserver(reserved_fragments, blocking=blocking, timeout=timeout)
#         self._name = name
#         # region init
#         self._skip_fragment_hashes = set()
#         self._overwrite = overwrite
#         self._compound = None  # type: Optional[Compound]
#
#         self._stream_hash = hashlib.sha256()
#         self._stream_size = CompoundSize(0)
#         self._fragment_data_buffer = bytes()
#         self._fragment_data_buffer_len = 0
#         self._payload_index = SequenceIndex(0)
#         self._fragment_payload_index = []
#         self._pending_fragments = []
#         self._pending_objects = PendingObjectsController()
#
#     def write(self, chunk):
#         # region write
#         # print("processing chunk", chunk)
#         if chunk:
#             self._stream_hash.update(chunk)
#             # stream_size += len(chunk)
#             # fill internal buffer of fragment_size size
#             self._fragment_data_buffer += chunk
#             self._fragment_data_buffer_len += len(chunk)
#         # if buffer is full, calculate fragment hash, size, reset buffer
#         # or if no chunk was read (EOF), process remaining buffer
#         if self._fragment_data_buffer_len >= self._fragment_size or (not chunk and self._fragment_data_buffer_len > 0):
#             self.flush()
#         # if not chunk and self._fragment_data_buffer_len <= 0:
#         #     break
#
#     def flush(self):
#         if self._fragment_data_buffer_len <= self._fragment_size:
#             fragment_data = self._fragment_data_buffer
#             self._fragment_data_buffer = bytes()
#             self._fragment_data_buffer_len = 0
#         else:
#             fragment_data, self._fragment_data_buffer = split_bytes(self._fragment_data_buffer, self._fragment_size)
#             self._fragment_data_buffer_len -= self._fragment_size
#         _fragment_payload_size = FragmentPayloadSize(len(fragment_data))
#         self._stream_size += _fragment_payload_size
#         fragment_data = encapsulate(self._compresser, self._wrapper, self._compress_type, self._wrap_type,
#                                     fragment_data)
#         fragment_hash = FragmentHash(hashlib.sha256(fragment_data).digest())
#         self._fragment_reserver.reserveOne(fragment_hash)
#         _fragment_size = FragmentSize(len(fragment_data))
#         fragment_hash_in_skip_fragment_hashes = fragment_hash in self._skip_fragment_hashes
#         # check if fragment already exists, if yes, no need to upload
#         if fragment_hash_in_skip_fragment_hashes:
#             fragment = self._meta.getFragmentByPayloadHash(fragment_hash)
#         else:
#             try:
#                 fragment = self._meta.getFragmentByPayloadHash(fragment_hash)
#             except NotExistingException:
#                 fragment = self._meta.makeFragment(fragment_hash, _fragment_size,
#                                                    _fragment_payload_size)
#                 # if not, build new fragment
#                 # build resource for upload
#                 # upload new resource
#                 self._pending_fragments.append(fragment)
#                 self._pending_objects.addFragment(fragment)
#                 self._fragment_cache.addFragment(fragment_data, fragment)
#
#             self._skip_fragment_hashes.add(fragment_hash)
#             # self.cache_meta.makeFragmentResourceMapping(fragment.fragment_id, resource.resource_id)
#         # remember fragment id and payload index
#         self._fragment_payload_index.append((fragment.fragment_id, self._payload_index))
#         self._payload_index += 1


def openWritableCompound(meta, fragment_cache, compound_am, fragment_am, fragment_size, wrapper, compresser, wrap_type,
                         compress_type, pending_objects, name, compound_type, overwrite=False, compound=None, append=False,
                         blocking=True, timeout=None):
    # type: (MetaDBInterface, FragmentCache, AccessManager[CompoundName], AccessManager[FragmentHash], int, AutoWrapper, AutoCompressor, WrappingType, CompressionType, PendingObjectsController, CompoundName, CompoundType, bool, Optional[Compound], bool, bool, Optional[float]) -> WritableCompound
    # print('openWritableCompound', name)
    if append:
        # Appending would require either the updating of a hash, sourced from its calculated value (not possible)
        # or to read in all the old data to have a hash object with the correct hash-state to generate the correct
        # overall compound hash
        raise NotImplementedError("appending is currently not possible.")
    if compound:
        assert compound.compound_name == name
    compound_reserver = ExclusiveAccessContext(compound_am, name, blocking=blocking, timeout=timeout)
    fragment_reserver = ParallelMassReserver(fragment_am, blocking=blocking, timeout=timeout)
    # if append:
    #     overwrite = True
    with compound_reserver:
        with meta:
            if compound:
                meta_has_compound = True
            else:
                meta_has_compound = meta.hasCompoundWithName(name)
            if compound:
                fragment_hashes = list(meta.getFragmentHashesNeededForCompound(compound.compound_id))
                fragment_reserver.reserveAll(*fragment_hashes)
            elif meta_has_compound and not overwrite:
                raise CompoundAlreadyExistsException(
                    "compound already exists (maybe different payload), not allowed to overwrite")
            elif meta_has_compound and overwrite:
                compound = meta.getCompoundByName(name)
                fragment_hashes = list(meta.getFragmentHashesNeededForCompound(compound.compound_id))
                fragment_reserver.reserveAll(*fragment_hashes)
            else:
                compound = None

        w_c = WritableCompound(meta, fragment_cache, compound_reserver, fragment_reserver, fragment_size, wrapper,
                                compresser, wrap_type, compress_type, pending_objects, name, compound_type, compound)
    # if append and meta_has_compound:
    #     sequence_indexes_fragments = meta.getSequenceIndexSortedFragmentsForCompound(meta.getCompoundByName(name).compound_id)
    #     for sequence_index, fragment in sequence_indexes_fragments:
    #         fragment.fragment_id = None
    #         w_c._fragment_payload_index.append((fragment, sequence_index))
    #     w_c._payload_index = len(sequence_indexes_fragments)
        return w_c


class WritableCompound(BinaryIO):

    def __init__(self, meta, fragment_cache, compound_reserver, fragment_reserver, fragment_size, wrapper, compresser,
                 wrap_type, compress_type, pending_objects, name, compound_type, compound=None):
        # type: (MetaDBInterface, FragmentCache, AccessContext[CompoundName], MassReserver[FragmentHash], int, AutoWrapper, AutoCompressor, WrappingType, CompressionType, PendingObjectsController, CompoundName, CompoundType, Optional[Compound]) -> None
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
        self._compound = compound  # optional, if given, overwrite this compound
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
        self._test_initial = self._fragment_cache._in_context
        # self._fragment_cache.__enter__()
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
        # try:
        #     if exc_tb:
        #         pass
        #         # self._clean_on_error(exc_type)
        #     else:
        #         # noinspection PyBroadException
        #         try:
        #             if not self._closed:
        #                 self._close()
        #         except Exception as e:
        #             exc_type = type(e)
        #             exc_tb = e.__traceback__
        #             # self._clean_on_error(exc_type)
        # finally:
        #     # self._fragment_cache.__exit__(exc_type, exc_val, exc_tb)
        #     self._fragment_reserver.__exit__(exc_type, exc_val, exc_tb)
        #     self._compound_reserver.__exit__(exc_type, exc_val, exc_tb)
        # assert self._test_initial == self._fragment_cache._in_context
        # # print(self._fragment_cache._in_context, self._test_initial)

    def write(self, chunk):
        assert type(chunk) in (bytes, bytearray)
        # print(threading.current_thread().name)
        if self.__debug:
            print('WritableCompound write', self._name, len(chunk))
        # print('WritableCompound write', self._name, len(chunk))

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
        # self.__exit__(None, None, None)

    def _close(self):
        try:
            if self.__debug:
                print('WritableCompound _close', self._name)
            # flush internal buffer completely
            self.flush()
            # process compound metadata
            stream_hash = self._stream_hash.digest()
            if not self._compound:
                # Todo: pending fragments+compounds
                #  create objects in python, if fragments are flushed by fcache, create table entries
                #  if pending objects gives ok to fcache that all fragments of a compound are written to storage,
                #  then write(create or update) the compound to database
                self._pending_compound = Compound(self._name, self._compound_type, stream_hash,
                                                  self._stream_size,
                                                  CompoundWrappingType(self._wrap_type),
                                                  CompoundCompressionType(self._compress_type))
                compound = self._pending_compound
                # compound = self._meta.makeCompound(self._name, self._compound_type, stream_hash,
                #                                    self._stream_size,
                #                                    CompoundWrappingType(self._wrap_type),
                #                                    CompoundCompressionType(self._compress_type))
                # self._pending_compound = compound
                # assert self.cache_meta.getCompoundByName(name)
                # assert self.cache_meta.hasCompoundWithName(name)
            else:
                self._pending_compound = Compound(self._name, self._compound_type, stream_hash,
                                                  self._stream_size,
                                                  CompoundWrappingType(self._wrap_type),
                                                  CompoundCompressionType(self._compress_type))
                compound = self._pending_compound
                # compound = self._meta.updateCompound(self._name, self._compound_type, stream_hash,
                #                                      self._stream_size,
                #                                      CompoundWrappingType(self._wrap_type),
                #                                      CompoundCompressionType(self._compress_type))
            assert compound.compound_size == self._stream_size
            # if compound.compound_size > 0:
            #     assert len(self._pending_fragments) > 0
            if self._pending_compound:
                self._pending_objects.addCompound(self._pending_compound, self._pending_fragments, self._fragment_payload_index)
            # map fragment ids and indexes to compound
            # moved to fragment cache
            # self._meta.setFragmentsMappingForCompound(compound.compound_id, self._fragment_payload_index)
            self._fragment_cache.flush()
            self._closed = True
        finally:
            self._unreserve()
            pass
            # self._fragment_reserver.unreserveAll()
            # self._compound_reserver.__exit__(None, None, None)

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
        # _fragment_payload_size = FragmentPayloadSize(len(fragment_data))
        # self._stream_size += _fragment_payload_size
        # fragment_data = encapsulate(self._compresser, self._wrapper, self._compress_type, self._wrap_type,
        #                             fragment_data)
        # fragment_hash = FragmentHash(hashlib.sha256(fragment_data).digest())
        # self._fragment_reserver.reserveOne(fragment_hash)
        # fragment_size = FragmentSize(len(fragment_data))
        # fragment_hash_in_skip_fragment_hashes = fragment_hash in self._skip_fragment_hashes
        # # check if fragment already exists, if yes, no need to upload
        # if fragment_hash_in_skip_fragment_hashes:
        #     fragment = self._meta.getFragmentByPayloadHash(fragment_hash)
        # else:
        #     try:
        #         fragment = self._meta.getFragmentByPayloadHash(fragment_hash)
        #     except NotExistingException:
        #         fragment = self._meta.makeFragment(fragment_hash, fragment_size,
        #                                            _fragment_payload_size)
        #         # if not, build new fragment
        #         # build resource for upload
        #         # upload new resource
        #         self._pending_fragments.append(fragment)
        #         self._pending_objects.addFragment(fragment)
        #         self._fragment_cache.addFragment(fragment_data, fragment)

        # self._skip_fragment_hashes.add(fragment_hash)
        # self.cache_meta.makeFragmentResourceMapping(fragment.fragment_id, resource.resource_id)
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
