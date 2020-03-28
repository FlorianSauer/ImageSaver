import hashlib
import warnings
from typing import Optional, Union, Type, Tuple, Generator, List, BinaryIO

import humanfriendly

from ImageSaverLib4.Encapsulation import (makeWrappingType, makeCompressingType, CompressionType, WrappingType,
                                          BaseWrapper, BaseCompressor, decapsulate)
from ImageSaverLib4.Encapsulation.Compressors.AutoCompressor import AutoCompressor
from ImageSaverLib4.Encapsulation.Compressors.Types import PassThroughCompressor, ZLibCompressor
from ImageSaverLib4.Encapsulation.Wrappers.AutoWrapper import AutoWrapper
from ImageSaverLib4.Encapsulation.Wrappers.Types import PassThroughWrapper
from ImageSaverLib4.Errors import (CompoundManipulatedException, ResourceMissingException,
                                   FragmentMissingException, CompoundAlreadyExistsException,
                                   CompoundNotExistingException, FragmentManipulatedException)
from ImageSaverLib4.FragmentCache import FragmentCache
from ImageSaverLib4.Helpers import chunkiterable_gen, get_sha256_of_stream
from ImageSaverLib4.Helpers.FileLikeIterator import FileLikeIterator
from ImageSaverLib4.Helpers.NotifyCounter import (AccessManager, ParallelMassReserver, ParallelAccessContext,
                                                  ExclusiveMassReserver)
from ImageSaverLib4.Helpers.NotifyCounter import ExclusiveAccessContext
from ImageSaverLib4.Helpers.SizedGenerator import SizedGenerator
from ImageSaverLib4.Helpers.TqdmReporter import TqdmUpTo
from ImageSaverLib4.Helpers.WritableStream import openWritableCompound, WritableCompound
from ImageSaverLib4.MetaDB.Errors import NotExistingException
from ImageSaverLib4.MetaDB.MetaDB import MetaDBInterface
from ImageSaverLib4.MetaDB.Types.Compound import CompoundName, CompoundType, Compound, CompoundHash
from ImageSaverLib4.MetaDB.Types.Fragment import FragmentHash, FragmentSize, FragmentID
from ImageSaverLib4.MetaDB.Types.FragmentResourceMapping import FragmentOffset
from ImageSaverLib4.MetaDB.Types.Resource import ResourceName, ResourceID, ResourceSize, ResourceWrappingType, \
    ResourceCompressionType
from ImageSaverLib4.PendingObjectsController import PendingObjectsController
from ImageSaverLib4.Storage.StorageInterface import StorageInterface


class ImageSaver(object):
    def __init__(self, meta, storage, fragment_size=1000000, resource_size=None):
        # type: (MetaDBInterface, StorageInterface, Optional[Union[FragmentSize, int]], Optional[Union[ResourceSize, int]]) -> None
        if resource_size is None:
            resource_size = storage.getMaxSupportedResourceSize()
        else:
            assert resource_size <= storage.getMaxSupportedResourceSize()
        if fragment_size is None and resource_size < 1000000:
            fragment_size = resource_size
        elif fragment_size is None:
            fragment_size = 1000000
        self.storage = storage  # storage interface, can throw all kinds of exceptions during upload
        self.meta = meta  # type: MetaDBInterface
        self.fragment_size = fragment_size
        self.reserved_compounds = AccessManager(CompoundName)
        self.reserved_fragments = AccessManager(FragmentHash)
        self.reserved_resources = AccessManager(ResourceName)
        self.wrapper = AutoWrapper()
        self.compresser = AutoCompressor()
        self._wrap_type = makeWrappingType(PassThroughWrapper)  # type: WrappingType
        self._compress_type = makeCompressingType(ZLibCompressor)  # type: CompressionType
        self.pending_objects = PendingObjectsController()
        self._within_context = 0
        self.fragment_cache = FragmentCache(self.meta, self.storage, fragment_size,
                                            self.storage.getRequiredWrapType(),
                                            makeCompressingType(PassThroughCompressor),
                                            resource_size, self.pending_objects,
                                            self.wrapper, self.compresser,
                                            # resource_minimum_filllevel=1.0,
                                            debug=False)  # type: FragmentCache

    def __enter__(self):
        self._within_context += 1
        self.fragment_cache.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._within_context -= 1
        # print("islib", self._within_context, exc_type, exc_val, exc_tb)
        error = True
        try:
            self.fragment_cache.__exit__(exc_type, exc_val, exc_tb)
            error = False
        except Exception as e:
            exc_type = type(e)
            exc_tb = e.__traceback__
            # finally:
            if error:
                print("EXCEPTION CLEANUP, exception", e, "occured, removing pending compounds+fragments from meta...")
                # if exc_tb:
                #     print("catched exception", exc_type, exc_val, exc_tb)
                print("removing", len(self.pending_objects.getPendingCompounds()), "pending compounds",
                      "due to an error", exc_type)
                for c in self.pending_objects.getPendingCompounds():
                    print("removing pending compound", c.compound_name, "due to an error", exc_type)
                    self.meta.removeCompound(c.compound_id)
                    self.pending_objects.removeCompound(c)
                print("removing", len(self.pending_objects.getPendingFragments()), "pending fragments",
                      "due to an error", exc_type)
                self.meta.deleteFragments(self.pending_objects.getPendingFragments())
                for f in self.pending_objects.getPendingFragments():
                    self.pending_objects.removeFragment(f)
                assert len(self.pending_objects.getPendingCompounds()) == 0, repr(
                    self.pending_objects.getPendingCompounds())
                raise
        # if exc_tb:
        #     assert len(self.pending_objects.getPendingCompounds()) == 0, repr(
        #         self.pending_objects.getPendingCompounds())
        #     return False

    def flush(self):
        self.fragment_cache.flush(force=True)

    @property
    def defaultCompoundWrapper(self):
        # type: () -> WrappingType
        return self._wrap_type

    @defaultCompoundWrapper.setter
    def defaultCompoundWrapper(self, wrapper):
        # type: (Union[Type[BaseWrapper], BaseWrapper]) -> None
        self._wrap_type = wrapper.get_wrapper_type()

    def setDefaultCompoundWrapper(self, wrapper):
        # type: (Union[Type[BaseWrapper], BaseWrapper]) -> None
        self._wrap_type = wrapper.get_wrapper_type()

    @property
    def defaultResourceWrapper(self):
        # type: () -> Union[ResourceWrappingType, WrappingType] 
        return self.fragment_cache.resource_wrap_type

    @defaultResourceWrapper.setter
    def defaultResourceWrapper(self, wrapper):
        # type: (Union[Type[BaseWrapper], BaseWrapper]) -> None
        self.fragment_cache.resource_wrap_type = wrapper.get_wrapper_type()

    def setDefaultResourceWrapper(self, wrapper):
        # type: (Union[Type[BaseWrapper], BaseWrapper]) -> None
        """
        Resource Wrapper is set from given Storage, changing this can severely interfere with the storage and its
        functionality, rendering it unusable an making stored resource data unrestorable.
        """
        self.fragment_cache.resource_wrap_type = wrapper.get_wrapper_type()

    @property
    def defaultCompoundCompressor(self):
        # type: () -> CompressionType
        return self._compress_type
    
    @defaultCompoundCompressor.setter
    def defaultCompoundCompressor(self, compressor):
        # type: (Union[Type[BaseCompressor], BaseCompressor]) -> None
        self.fragment_cache.resource_compress_type = compressor.get_compressor_type()

    def setDefaultCompoundCompressor(self, compressor):
        # type: (Union[Type[BaseCompressor], BaseCompressor]) -> None
        self._compress_type = compressor.get_compressor_type()

    @property
    def defaultResourceCompressor(self):
        # type: () -> Union[ResourceCompressionType, CompressionType]
        return self.fragment_cache.resource_compress_type

    @defaultResourceCompressor.setter
    def defaultResourceCompressor(self, compressor):
        # type: (Union[Type[BaseCompressor], BaseCompressor]) -> None
        self.fragment_cache.resource_compress_type = compressor.get_compressor_type()

    def setDefaultResourceCompressor(self, compressor):
        # type: (Union[Type[BaseCompressor], BaseCompressor]) -> None
        self.fragment_cache.resource_compress_type = compressor.get_compressor_type()

    @property
    def wrap_type(self):
        # type: () -> str
        warnings.warn(
            'wrap_type property is deprecated. Use setDefaultCompoundWrapper.',
            DeprecationWarning,
        )
        return self._wrap_type

    @wrap_type.setter
    def wrap_type(self, value):
        # type: (WrappingType) -> None
        warnings.warn(
            'wrap_type property is deprecated. Use setDefaultCompoundWrapper.',
            DeprecationWarning,
        )
        self._wrap_type = value

    @property
    def compress_type(self):
        warnings.warn(
            'compress_type property is deprecated. Use setDefaultCompoundCompressor.',
            DeprecationWarning,
        )
        return self._compress_type

    @compress_type.setter
    def compress_type(self, value):
        # type: (CompressionType) -> None
        # print('property set', self._compress_type, value)
        warnings.warn(
            'compress_type property is deprecated. Use setDefaultCompoundCompressor.',
            DeprecationWarning,
        )
        self._compress_type = value

    def flushPending(self):
        if self._within_context == 0:
            self.__exit__(None, None, None)

    def changeFragmentSize(self, fragmentSize):
        # type: (int) -> None
        self.fragment_size = int(fragmentSize)

    def saveStream(self, stream, name, fragment_size=None, wrap_type=None, compress_type=None, blocking=True,
                   timeout=None, read_speed=None, compound_type=Compound.FILE_TYPE, overwrite=False,
                   pre_calc_stream_hash=False,
                   progressreporter=None):
        # type: (BinaryIO, str, Optional[int], Optional[WrappingType], Optional[CompressionType], bool, Optional[float], Optional[int], str, bool, bool, Optional[TqdmUpTo]) -> None
        name = CompoundName(name)
        compound_type = CompoundType(compound_type)
        if not wrap_type:
            wrap_type = self._wrap_type
        if not compress_type:
            compress_type = self._compress_type
        if not fragment_size:
            fragment_size = self.fragment_size
        else:
            fragment_size = int(fragment_size)
        if not read_speed:
            read_speed = fragment_size

        try:
            compound = self.meta.getCompoundByName(name)
        except NotExistingException:
            compound = None

        if compound:
            if overwrite and pre_calc_stream_hash:
                stream_hash = CompoundHash(get_sha256_of_stream(stream, read_speed))
                if compound.compound_hash == stream_hash:
                    raise CompoundAlreadyExistsException(
                        "compound already exists with same payload, overwrite not needed")
        w_c = openWritableCompound(self.meta, self.fragment_cache, self.reserved_compounds, self.reserved_fragments,
                                   fragment_size, self.wrapper, self.compresser, wrap_type, compress_type,
                                   self.pending_objects, name, compound_type, overwrite, compound, False,
                                   blocking, timeout)
        with w_c:
            stream_size = 0
            while True:
                chunk = stream.read(read_speed)
                if not chunk:
                    break
                stream_size += len(chunk)
                w_c.write(chunk)
                if progressreporter is not None:
                    progressreporter.update_to(stream_size)

    def openWritableCompound(self, name, compound_type=Compound.FILE_TYPE, wrap_type=None, compress_type=None,
                             fragment_size=None, overwrite=False, append=False, blocking=True, timeout=None):
        # type: (Union[str, CompoundName], Union[str, CompoundType], WrappingType, CompressionType, Union[int, FragmentSize], bool, bool, bool, Optional[float]) -> WritableCompound
        # create WritableCompound, which feeds into fragment cache
        # writable compound has no control of fragment cache and meta
        # it can only query for fragments or create fragments
        # meta should have a fragment lock, which locks fragment querying or creating
        # fragment cache must be able to return fragments if they are in his buffer, it can be possible that
        # the fragment is not yet flushed to a storage
        # reservation of a fragment must be started as soon as its created/hash is created, creating must be exclusive
        # after its added to the fragment cache the reservation type must be changed to parallel

        # if self.meta.hasCompoundWithName(name):
        #     compound = self.meta.getCompoundByName(name)
        # else:
        #     compound = None

        name = CompoundName(name)
        compound_type = CompoundType(compound_type)
        if not wrap_type:
            wrap_type = self._wrap_type
        if not compress_type:
            compress_type = self._compress_type
        if not fragment_size:
            fragment_size = self.fragment_size
        else:
            fragment_size = int(fragment_size)

        w_c = openWritableCompound(meta=self.meta,
                                   fragment_cache=self.fragment_cache,
                                   compound_am=self.reserved_compounds,
                                   fragment_am=self.reserved_fragments,
                                   fragment_size=fragment_size,
                                   wrapper=self.wrapper,
                                   compresser=self.compresser,
                                   wrap_type=wrap_type,
                                   compress_type=compress_type,
                                   pending_objects=self.pending_objects,
                                   name=name,
                                   compound_type=compound_type,
                                   overwrite=overwrite,
                                   append=append,
                                   blocking=blocking,
                                   timeout=timeout)
        return w_c

    def saveBytes(self, data, name, fragment_size=None, wrap_type=None, compress_type=None, blocking=True,
                  timeout=None, compound_type=Compound.FILE_TYPE, overwrite=False,
                  ):
        # type: (bytes, str, Optional[int], WrappingType, CompressionType, bool, Optional[float], CompoundType, bool) -> None
        f = self.openWritableCompound(name, compound_type, wrap_type, compress_type, fragment_size,
                                      overwrite, False, blocking, timeout)
        with f:
            f.write(data)

    def loadCompoundBytes(self, name, blocking=True, timeout=None, progressreporter=None):
        # type: (str, bool, Optional[float], Optional[TqdmUpTo]) -> bytes
        return bytes().join(self.loadCompound(name, blocking=blocking, timeout=timeout,
                                              progressreporter=progressreporter))

    def loadCompound(self, name, blocking=True, timeout=None, progressreporter=None):
        # type: (str, bool, Optional[float], Optional[TqdmUpTo]) -> Generator[bytes, None, None]
        # Todo: add cache / intelligent cache where duplicate fragments are detected at beginning

        name = CompoundName(name)
        downloaded_data = 0
        hasher = hashlib.sha256()
        with self.meta:
            # reserve compound name
            with ParallelAccessContext(self.reserved_compounds, name, blocking=blocking, timeout=timeout):
                # check if compound exists
                compound = self.pending_objects.getPendingCompoundWithName(name)
                if compound:
                    _fragment_hashes = self.pending_objects.getFragmentsNeededForPendingCompound(compound.compound_name)
                    if compound.compound_size > 0:
                        assert _fragment_hashes is not None and len(_fragment_hashes) > 0
                    fragment_hashes = [f.fragment_hash for f, _ in _fragment_hashes]
                else:
                    if not self.meta.hasCompoundWithName(name):
                        # if not, raise error
                        raise CompoundNotExistingException("no compound found with name " + repr(name))
                    compound = self.meta.getCompoundByName(name)
                    fragment_hashes = list(self.meta.getFragmentHashesNeededForCompound(compound.compound_id))
                # print('Compound', compound.compound_id)
                # reserve Payload mapped to compound
                # if compound.compound_type == compound.FILE_TYPE and not compound.payload_id:
                #     raise Exception("Payload of Compound vanished")
                # reserve all fragment names needed for compound
                # print('fragment hashes', [fh.hex() for fh in fragment_hashes])
                with ParallelMassReserver(self.reserved_fragments, *fragment_hashes, blocking=blocking,
                                          timeout=timeout) as fragment_reserver:
                    # iterate through fragments, sorted by index
                    sorted_fragments = self.pending_objects.getFragmentsNeededForPendingCompound(compound.compound_name)
                    if sorted_fragments is None:
                        sorted_fragments = list(self.meta.getSequenceIndexSortedFragmentsForCompound(
                            compound.compound_id))
                    else:
                        sorted_fragments = [(i, f) for f, i in sorted_fragments]
                    for sequence_index, fragment in sorted_fragments:
                        assert fragment.fragment_hash in fragment_hashes, repr(
                            (fragment.fragment_hash, 'not in', fragment_hashes))
                        assert self.reserved_fragments.managesValue(fragment.fragment_hash)
                    # print("sorted_fragments_resources size", len(sorted_fragments_resources))
                    for sequence_index, fragment in sorted_fragments:
                        # print("payload index", payload_index,
                        #       "fragment", fragment.fragment_id, fragment.fragment_payload_hash.hex(),
                        #       "resource", resource.resource_id, resource.resource_hash.hex())
                        fragment_data = self.fragment_cache.loadFragment(fragment)
                        fragment_reserver.unreserveOne(fragment.fragment_hash)
                        fragment_size = FragmentSize(len(fragment_data))
                        if fragment_size != fragment.fragment_size:
                            raise FragmentManipulatedException(
                                "downloaded fragment has a not expected size, expected " + str(
                                    fragment.fragment_size) + ' got ' + str(fragment_size))
                        fragment_hash = FragmentHash(hashlib.sha256(fragment_data).digest())
                        if fragment_hash != fragment.fragment_hash:
                            raise FragmentManipulatedException("downloaded fragment has a not expected hash")

                        fragment_data = decapsulate(self.compresser, self.wrapper, compound.compression_type,
                                                    compound.wrapping_type,
                                                    fragment_data)
                        downloaded_data += len(fragment_data)

                        # print()
                        # print()
                        # print('???', type(progressreporter))
                        # bool(progressreporter)
                        if progressreporter is not None:
                            progressreporter.update_to(downloaded_data, tsize=compound.compound_size)
                        hasher.update(fragment_data)
                        yield fragment_data

        if hasher.digest() != compound.compound_hash:
            # print(hasher.digest(), compound.compound_hash)
            raise CompoundManipulatedException("Total Compound payload hash does not match the saved one in meta.")
        yield b''

    def openReadableCompound(self, name, blocking=True, timeout=None, progressreporter=None):
        # type: (str, bool, Optional[float], Optional[TqdmUpTo]) -> FileLikeIterator
        return FileLikeIterator(self.loadCompound(name, blocking, timeout, progressreporter))

    def collectGarbage(self, keep_fragments=True, keep_resources=False, keep_unreferenced_resources=True,
                       blocking=True, timeout=None, progressreporter_fragments=None, progressreporter_resources=None):
        # type: (bool, bool, bool, bool, Optional[float], Optional[TqdmUpTo], Optional[TqdmUpTo]) -> None
        """
        deletes all Fragments, which are not mapped to a Compound
        deletes all Resources on storage, which are not mapped to a Fragment
        deletes all Resources on storage, which are not referenced with a Resource in cache_meta
        """

        # region rework
        if not keep_fragments:
            # get list of not referenced fragments
            # reserve them, delete them
            with self.meta:
                # print("getting fragments")
                unreferenced_fragments = list(self.meta.getUnreferencedFragments())
                # print("getting fragments len")
                unreferenced_fragments_count = len(unreferenced_fragments)
                deleted_count = 0
                if progressreporter_fragments is not None:
                    progressreporter_fragments.update_to(deleted_count, tsize=unreferenced_fragments_count)
                # unreferenced_fragment_hashes = unreferenced_fragments.add_layer(lambda gen: (f.fragment_hash for f in gen))
                with ExclusiveMassReserver(self.reserved_fragments,  # values_gen=unreferenced_fragment_hashes,
                                           blocking=blocking, timeout=timeout) as reserver:
                    # print("pre loop")
                    for chunk_index, fragments_chunk in enumerate(chunkiterable_gen(unreferenced_fragments, 10)):
                        fragments_chunk_len = len(fragments_chunk)
                        # for fragment in (f for f in fragments_chunk if f):  # filter None values from chunk tuples
                        # print("making fragment list of 10")
                        fragments_chunk = list((f for f in fragments_chunk if f))
                        fragments_chunk_len = len(fragments_chunk)

                        # print("reserving")
                        reserver.reserveAll(*[f.fragment_hash for f in fragments_chunk])

                        # print("deleting")
                        self.meta.deleteFragments(fragments_chunk)
                        deleted_count += len(fragments_chunk)
                        # print("status")
                        if progressreporter_fragments is not None:
                            progressreporter_fragments.update_to(deleted_count, tsize=unreferenced_fragments_count)

                        # print("unreserving")
                        # reserver.unreserveAll()
                    # self.cache_meta.deleteFragments(unreferenced_fragments)
                    # self.cache_meta.deleteUnreferencedFragments()

        with self.meta:
            resources = []  # type: List[Tuple[ResourceName, Optional[ResourceID]]]
            if not keep_resources:
                # this leaves resources, which only store a few/one fragment
                # get list of resources, which are not used by any fragment
                # reserve them, delete them
                unreferenced_resources = self.meta.getUnreferencedResources()
                resources += [(r.resource_name, r.resource_id) for r in unreferenced_resources]
                # unreferenced_resource_names = [r.resource_name for r in unreferenced_resources]
                # with ExclusiveMassReserver(self.reserved_resources, *unreferenced_resource_names,
                #                            blocking=blocking, timeout=timeout) as reserver:
                #     for resource in unreferenced_resources:
                #         self.storage.deleteResource(resource.resource_name)
                #         self.cache_meta.deleteResourceByID(resource.resource_id)
                #         reserver.unreserveOne(resource.resource_name)

            if not keep_unreferenced_resources:
                # this leaves only resources, which are needed by at least one fragment
                # now some resources might be in storage, which are not referenced in cache_meta, delete them
                resource_names = set(self.storage.listResourceNames()) - set(self.meta.getAllResourceNames())
                resources += [(rn, None) for rn in resource_names]
                # if not keep_unreferenced_resources:
                #     with ExclusiveMassReserver(self.reserved_resources, blocking=blocking,
                #                                timeout=timeout) as resource_reserver:
                #         resource_names = set(self.storage.listResourceNames()) - set(self.cache_meta.getAllResourceNames())
                #         resource_reserver.reserveAll(*resource_names)
                #         for resource_name in resource_names:
                #             print("removing unreferenced resource", resource_name, "from storage")
                #             self.storage.deleteResource(resource_name)

            resource_names = [t[0] for t in resources]
            resource_count = len(resources)
            # print(resource_names)
            with ExclusiveMassReserver(self.reserved_resources, *resource_names, blocking=blocking,
                                       timeout=timeout) as resource_reserver:
                for index, name_id in enumerate(resources):
                    resource_name, resource_id = name_id
                    self.storage.deleteResource(resource_name)
                    if resource_id:
                        self.meta.deleteResourceByID(resource_id)
                    resource_reserver.unreserveOne(resource_name)
                    # print(resource_name, resource_count)
                    if progressreporter_resources is not None:
                        progressreporter_resources.update_to(index + 1, tsize=resource_count)
        # endregion

        #
        #
        # # get list of not needed payloads
        # # reserve them
        # # garbage collect them
        # # get list of not needed fragments
        # # reserve them
        # # garbage collect them
        # # get list of not needed resources
        # # reserve them
        # # garbage collect them + delete them in storage
        #
        # with self.cache_meta:
        #     # self.cache_meta.collectGarbage(True, True)
        #     if not keep_resources:
        #         unneeded_fragments = self.cache_meta.getUnneededFragments()
        #         unneeded_fragments_hashes = [f.fragment_hash for f in unneeded_fragments]
        #         unneeded_fragments_len = len(unneeded_fragments)
        #         # for fragment in unneeded_fragments:
        #         #     if unneeded_fragments_hashes.count(fragment.fragment_payload_hash) > 1:
        #         #         print("duplicate fragment", fragment.fragment_id)
        #         # assert len(unneeded_fragments) == len(set(unneeded_fragments_hashes))
        #         with ExclusiveMassReserver(self.reserved_fragments,
        #                                    *unneeded_fragments_hashes,
        #                                    blocking=blocking, timeout=timeout) as reserved_fragments:
        #             for index, fragment in enumerate(unneeded_fragments):
        #                 # print(fragment.fragment_id)
        #                 try:
        #                     resource = self.cache_meta.getResourceForFragment(fragment.fragment_id)
        #                 except NotExistingException:
        #                     continue
        #                 finally:
        #                     reserved_fragments.unreserveOne(fragment.fragment_hash)
        #                     if progressreporter_resources is not None:
        #                         progressreporter_resources.update_to(index+1, tsize=unneeded_fragments_len)
        #                 # print("removing resource", resource.resource_id, resource.resource_name, "from storage")
        #                 self.storage.deleteResource(resource.resource_name)
        #                 self.cache_meta.deleteResource(resource.resource_id)
        #     self.cache_meta.collectGarbage(keep_fragments, keep_resources)
        #     if not keep_unreferenced_resources:
        #         with ExclusiveMassReserver(self.reserved_resources, blocking=blocking, timeout=timeout) as resource_reserver:
        #             for resource_name in (set(self.storage.listResourceNames()) - set(self.cache_meta.getAllResourceNames())):
        #                 resource_reserver.reserveOne(resource_name)
        #                 print("removing unreferenced resource", resource_name, "from storage")
        #                 self.storage.deleteResource(resource_name)
        #
        # pass

    def optimizeResourceSpace(self, unused_percentage=0.0, blocking=True, timeout=None, progressreporter=None):
        # type: (Optional[float], bool, Optional[float], Optional[TqdmUpTo]) -> None
        """
        Removes 'fragment-holes' insode of resource payload.
        Downloads resources and moves the remaining fragments to a new resource, which is then uploaded.
        """
        with self.meta:
            if unused_percentage is None:
                unused_percentage = 1.0 - self.fragment_cache.resource_minimum_filllevel
            assert 0 <= unused_percentage <= 1
            # get resource, get referenced fragment sizes of this resource
            # if sizes/resource.payload_size <= minimum_usage:
            # download resource, decapsulate, remove unknown parts, build new resource, upload
            resources_fragment_sizes = self.meta.getResourceWithReferencedFragmentSize()
            resources_fragment_sizes = [(r, f) for r, f in resources_fragment_sizes if
                                        (f / r.resource_payloadsize) < (1.0 - unused_percentage)]
            resources_count = len(resources_fragment_sizes)
            for index, (resource, fragment_sizes) in enumerate(resources_fragment_sizes):
                with ExclusiveAccessContext(self.reserved_resources, resource.resource_name, blocking=blocking,
                                            timeout=timeout):
                    # print('checking resource', resource, fragment_sizes)
                    hole_size = resource.resource_payloadsize - fragment_sizes
                    if (hole_size / resource.resource_payloadsize) >= unused_percentage:
                        if progressreporter is not None:
                            progressreporter.write(
                                'can optimize Resource ' + resource.resource_name + ', unused space: ' + "{:3.2f}".format(
                                    ((hole_size / resource.resource_payloadsize) * 100.0)) + ' %')
                        # print('can optimize Resource '+resource.resource_name+', unused space:',
                        #       (fragment_sizes / resource.resource_payloadsize) * 100.0, '%')
                        # noinspection PyProtectedMember
                        old_resource_data = self.fragment_cache.loadResource(resource)
                        new_resource_data = bytes()
                        fragments_count = 0
                        fragments_id_offset = []  # type: List[Tuple[FragmentID, FragmentOffset]]
                        fragments_buffer_size = 0
                        fragments_on_resource = list(self.meta.getFragmentsWithOffsetOnResource(resource.resource_id))
                        with ExclusiveMassReserver(self.reserved_fragments,
                                                   *(f.fragment_hash for f, _ in fragments_on_resource),
                                                   blocking=blocking, timeout=timeout):
                            for fragment, offset in fragments_on_resource:
                                fragment_data = old_resource_data[offset:offset + fragment.fragment_size]
                                new_resource_data += fragment_data
                                fragments_count += 1
                                fragments_id_offset.append(
                                    (fragment.fragment_id, FragmentOffset(fragments_buffer_size)))
                                # print("reordering", fragment.fragment_id, 'old offset', offset, 'new offset', fragments_buffer_size)
                                fragments_buffer_size += fragment.fragment_size
                            # noinspection PyProtectedMember
                            resource = self.fragment_cache._upload(new_resource_data, fragments_count=fragments_count)
                            self.meta.makeMultipleFragmentResourceMapping(resource.resource_id, fragments_id_offset)
                if progressreporter is not None:
                    progressreporter.update_to(index + 1, tsize=resources_count)

    def optimizeResourceUsage(self, fill_percentage=0.9, blocking=True, timeout=None, progressreporter=None):
        # type: (float, bool, Optional[float], Optional[TqdmUpTo]) -> None
        """
        iterates through resources and tries to combine their fragments into one. This operation might distribute
        Fragments needed for a compound across multiple resources.
        However after this operation less resources should be needed (also depends on fragmentcache policy and
        encapsulation)
        """
        # self.fragment_cache.debug = True
        orig_blacklist = set(self.fragment_cache.resource_reuse_blacklist)
        orig_policy = self.fragment_cache.policy
        with self.meta:
            try:
                self.fragment_cache.flush(force=True)
                self.fragment_cache.policy = self.fragment_cache.POLICY_FILL_ALWAYS
                with self.fragment_cache:
                    resources = self.meta.getAllResourcesSizeSorted()
                    resources = [r for r in resources if
                                 r.resource_payloadsize / self.fragment_cache.resource_size < fill_percentage]
                    resources_count = len(resources)
                    self.fragment_cache.resource_reuse_blacklist.clear()
                    self.fragment_cache.resource_reuse_blacklist = {r.resource_hash for r in resources}
                    sort_biggest = True
                    for index, resource in enumerate(resources):
                        # if resource.resource_payloadsize / self.fragment_cache.resource_size < fill_percentage:
                        # self.fragment_cache.resource_reuse_blacklist.add(resource.resource_hash)
                        with ExclusiveAccessContext(self.reserved_resources, resource.resource_name, blocking=blocking,
                                                    timeout=timeout):
                            # fragments = [f for f, _ in self.cache_meta.getFragmentsWithOffsetOnResource(resource.resource_id)]
                            fragments = self.meta.getFragmentsWithOffsetOnResource(resource.resource_id)
                            fragments = fragments.add_layer(lambda gen: (f for f, _ in gen))
                            fragments = sorted(list(fragments), key=lambda f: f.fragment_size, reverse=sort_biggest)
                            sort_biggest = not sort_biggest
                            with ExclusiveMassReserver(self.reserved_fragments, *(f.fragment_hash for f in fragments),
                                                       blocking=blocking, timeout=timeout):
                                for fragment in fragments:
                                    fragment_data = self.fragment_cache.loadFragment(fragment)
                                    self.fragment_cache.addFragment(fragment_data, fragment, readd=True)
                        if progressreporter is not None:
                            progressreporter.update_to(index + 1, tsize=resources_count)
                        # self.fragment_cache.resource_reuse_blacklist.remove(resource.resource_hash)
            finally:
                self.fragment_cache.resource_reuse_blacklist = orig_blacklist
                self.fragment_cache.policy = orig_policy

    def defragmentResources(self, progressreporter=None):
        # type: (Optional[TqdmUpTo]) -> None
        self.fragment_cache.flush(force=True)
        orig_fc_policy = self.fragment_cache.policy
        self.fragment_cache.policy = self.fragment_cache.POLICY_PASS
        try:
            fragment_gen = self.meta.getAllFragmentsSortedByCompoundUsage()
            fragment_gen_len = len(fragment_gen)
            unneeded_fragment_gen = self.meta.getUnneededFragments()
            unneeded_fragment_gen_len = len(unneeded_fragment_gen)
            fragments_count = fragment_gen_len + unneeded_fragment_gen_len
            for index, fragment in enumerate(fragment_gen):
                fragment_data = self.fragment_cache.loadFragment(fragment)
                self.fragment_cache.addFragment(fragment_data, fragment, readd=True)
                if progressreporter is not None:
                    progressreporter.update_to(index + 1, tsize=fragments_count)
            for index, fragment in enumerate(unneeded_fragment_gen):
                fragment_data = self.fragment_cache.loadFragment(fragment)
                self.fragment_cache.addFragment(fragment_data, fragment, readd=True)
                if progressreporter is not None:
                    progressreporter.update_to(fragment_gen_len + index + 1, tsize=fragments_count)
            progressreporter.update_to(fragments_count, tsize=fragments_count)
            self.fragment_cache.flush(force=True)
        finally:
            self.fragment_cache.policy = orig_fc_policy

    def checkStorageConsistency(self):
        """
        checks if all required resources are present on storage.
        Does not check resource data, only resource names
        """
        with self.meta:
            meta_set = set(self.meta.getAllResourceNames())
            storage_set = set(self.storage.listResourceNames())
            # at least cache_meta must be in storage set, if storage set is bigger, this only means that there is garbage/unknown
            # files stored in storage
            if not meta_set.issubset(storage_set):
                difference = meta_set.difference(storage_set)
                print("Meta set", len(meta_set))
                print(meta_set)
                print("storage set", len(storage_set))
                print(storage_set)
                print("difference", len(difference))
                print(difference)
                for i in difference:
                    assert i not in storage_set
                raise ResourceMissingException("Storage is missing one or multiple Resources referenced by cache_meta")
            return True

    def checkMetaConsistencyResourcelessFragments(self):
        """
        checks if all fragments have a reference to a resource via FragmentResourceMapping
        """
        with self.meta:
            resourceless_fragments = self.meta.getAllFragmentsWithNoResourceLink()
            if len(resourceless_fragments) > 0:
                raise ResourceMissingException(
                    "Meta is missing one or multiple Resources where Fragments are linked to")
        return True

    def checkMetaConsistencyFragmentlessCompounds(self):
        """
        checks if all compounds have a reference to fragments via CompoundFragmentMapping
        """
        with self.meta:
            fragmentless_compounds = self.meta.getAllCompoundsWithNoFragmentLink()
            if len(fragmentless_compounds) > 0:
                raise FragmentMissingException(
                    "Meta is missing one or multiple Fragments where Compounds are linked to")
        return True

    def checkStorageConsistencyByStorageContent(self, progressreporter=None):
        # type: (Optional[TqdmUpTo]) -> None
        """
        checks if all required resources are present on storage and if all downloaded resources yield the correct
        ResourceHash
        """
        with self.meta:
            resource_len_gen = self.meta.getAllResources()
            for index, resource in enumerate(resource_len_gen):
                self.fragment_cache.loadResource(resource)
                if progressreporter is not None:
                    progressreporter.update_to(index + 1, tsize=len(resource_len_gen))

    def checkConsistencyOfAllCompounds(self, progressreporter=None):
        # type: (TqdmUpTo) -> None
        """
        checks if all saved Compounds can be correctly reassembled by downloading everything to a /dev/null file
        """
        total_compound_size = self.getTotalCompoundSize()
        with self.meta:
            processed_size = 0
            for compound in self.meta.getAllCompounds():
                if compound.compound_type == Compound.DIR_TYPE:
                    self.loadCompoundBytes(compound.compound_name)
                    continue
                if progressreporter is not None:
                    progressreporter.write("Checking \""
                                           + compound.compound_name
                                           + "\" ("
                                           + humanfriendly.format_size(compound.compound_size)
                                           + ")")
                for chunk in self.loadCompound(compound.compound_name):
                    processed_size += len(chunk)
                    if progressreporter is not None:
                        progressreporter.update_to(processed_size, tsize=total_compound_size)

    def listCompounds(self, type_filter=None, order_alphabetically=False, starting_with=None, ending_with=None,
                      slash_count=None):
        # type: (Optional[CompoundType], bool, Optional[str], Optional[str], Optional[int]) -> SizedGenerator[Compound]
        return self.meta.getAllCompounds(type_filter, order_alphabetically, starting_with, ending_with, slash_count)
        # if type_filter:
        #     return [c for c in self.cache_meta.getAllCompounds() if c.compound_type == type_filter]
        # else:

    def getTotalCompoundSize(self):
        # type: () -> int
        return self.meta.getTotalCompoundSize()
        pass

    def getTotalCompoundCount(self, with_type=None):
        # type: (Optional[CompoundType]) -> int
        return self.meta.getTotalCompoundCount(with_type=with_type)
        pass

    def getUniqueCompoundSize(self):
        # type: () -> int
        return self.meta.getUniqueCompoundSize()
        pass

    def getUniqueCompoundCount(self):
        # type: () -> int
        return self.meta.getUniqueCompoundCount()
        pass

    def getTotalFragmentSize(self):
        # type: () -> int
        return self.meta.getTotalFragmentSize()

    def getTotalFragmentCount(self):
        # type: () -> int
        return self.meta.getTotalFragmentCount()
        pass

    def getTotalResourceSize(self):
        # type: () -> int
        return self.meta.getTotalResourceSize()
        pass

    def getTotalResourceCount(self):
        # type: () -> int
        return self.meta.getTotalResourceCount()
        pass

    def deleteCompound(self, name, blocking=True, timeout=None):
        # type: (str, bool, Optional[float]) -> None
        name = CompoundName(name)
        with self.meta:
            # with exclusive reserve name....
            with ExclusiveAccessContext(self.reserved_compounds, name, blocking=blocking, timeout=timeout):
                self.pending_objects.removeCompoundByName(name)
                # check if compound by name exist
                if not self.meta.hasCompoundWithName(name):
                    # if no, error
                    raise CompoundNotExistingException("compound '" + str(name) + "' does not exist")
                compound = self.meta.getCompoundByName(name)
                # remove compound-payload mapping
                # remove compound
                self.meta.removeCompound(compound.compound_id)

    def deleteCompoundStartingWith(self, name, compound_type=None, blocking=True, timeout=None):
        # type: (str, Optional[CompoundType], bool, Optional[float]) -> None
        with self.meta:
            # compounds = self.meta.getCompoundByStartingName(name, compound_type)
            compounds = self.meta.getAllCompounds(type_filter=compound_type, starting_with=name)
            for compound in compounds:
                with ExclusiveAccessContext(self.reserved_compounds, compound.compound_name, blocking=blocking,
                                            timeout=timeout):
                    # remove compound-payload mapping
                    # remove compound
                    self.meta.removeCompound(compound.compound_id)

    def renameCompound(self, old_name, new_name, blocking=True, timeout=None):
        # type: (str, str, bool, Optional[float]) -> None
        old_name = CompoundName(old_name)
        new_name = CompoundName(new_name)
        with self.meta:
            # with exclusive reserve name....
            with ExclusiveAccessContext(self.reserved_compounds, old_name, blocking=blocking, timeout=timeout):
                # check if compound by old_name exists
                if not self.meta.hasCompoundWithName(old_name):
                    # if no, error
                    raise Exception("compound does not exist")
                old_compound = self.meta.getCompoundByName(old_name)
                # with exclusive reserve name....
                with ExclusiveAccessContext(self.reserved_compounds, new_name, blocking=blocking, timeout=timeout):
                    # check if compound by new_name exists
                    if self.meta.hasCompoundWithName(old_name):
                        # if yes, error
                        raise Exception("compound already exist")
                    # update compound name
                    self.meta.renameCompound(old_compound.compound_name, new_name)

    def copyCompound(self, src_name, dst_name, overwrite=True, blocking=True, timeout=None):
        # type: (str, str, bool, bool, Optional[float]) -> None
        src_name = CompoundName(src_name)
        dst_name = CompoundName(dst_name)
        with self.meta:
            with ExclusiveMassReserver(self.reserved_compounds, src_name, dst_name, blocking=blocking, timeout=timeout):
                if not overwrite and self.meta.hasCompoundWithName(dst_name):
                    raise CompoundAlreadyExistsException("Destination compound already exists")
                src_compound = self.meta.getCompoundByName(src_name)
                needed_fragments_with_index = list(((t[1], t[0]) for t in self.meta.getSequenceIndexSortedFragmentsForCompound(src_compound.compound_id)))
                with ExclusiveMassReserver(self.reserved_fragments, values_gen=(t[0].fragment_hash for t in needed_fragments_with_index)):
                    dst_compound = self.meta.makeCompound(dst_name, src_compound.compound_type,
                                                          src_compound.compound_hash, src_compound.compound_size,
                                                          src_compound.wrapping_type, src_compound.compression_type)
                    self.meta.addOverwriteCompoundAndMapFragments(dst_compound, needed_fragments_with_index)

    def wipeAll(self, blocking=True, timeout=None, collect_garbage=False):
        # type: (bool, Optional[float], bool) -> None
        with self.meta:
            with ExclusiveMassReserver(self.reserved_compounds, blocking=blocking,
                                       timeout=timeout) as reserved_compounds:
                compound_names = self.meta.getAllCompoundNames()
                reserved_compounds.reserveAll(*compound_names)
                # self.storage.wipeResources()
                self.meta.truncateAllCompounds()
            if collect_garbage:
                self.collectGarbage()

    def getMultipleUsedFragmentsCount(self):
        # type: () -> int
        return self.meta.getDuplicateFragmentsCount()

    def getMultipleUsedCompoundsCount(self, compound_type=None):
        # type: (Optional[CompoundType]) -> int
        return self.meta.getMultipleUsedCompoundsCount(compound_type)

    def getSavedBytesByMultipleUsedFragments(self):
        # type: () -> int
        return self.meta.getSavedBytesByDuplicateFragments()

    def getSavedBytesByMultipleUsedCompounds(self):
        # type: () -> int
        return self.meta.getSavedBytesByMultipleUsedCompounds()

    def hasCompoundWithName(self, name):
        # type: (str) -> bool
        compound = self.pending_objects.getPendingCompoundWithName(CompoundName(name))
        if compound:
            return True
        return self.meta.hasCompoundWithName(CompoundName(name))

    def getCompoundWithName(self, name):
        # type: (str) -> Compound
        compound = self.pending_objects.getPendingCompoundWithName(CompoundName(name))
        if compound:
            return compound
        return self.meta.getCompoundByName(CompoundName(name))

    def getUnneededFragmentCount(self):
        # type: () -> int
        return len(self.meta.getUnneededFragments())

    def getUnneededFragmentSize(self):
        # type: () -> int
        gen = self.meta.getUnneededFragments()
        gen = gen.add_layer(lambda _gen: (f.fragment_size for f in _gen))
        return sum(gen)
