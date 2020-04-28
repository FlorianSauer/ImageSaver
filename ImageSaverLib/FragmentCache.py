import hashlib
import sys
from collections import OrderedDict
from threading import RLock
from typing import List, Tuple, Dict, Optional, Iterable, Union, Set, Callable

import binpacking
import humanfriendly

from ImageSaverLib.Encapsulation import encapsulate, decapsulate, WrappingType, CompressionType
from ImageSaverLib.Encapsulation.Compressors.AutoCompressor import AutoCompressor
from ImageSaverLib.Encapsulation.Wrappers.AutoWrapper import AutoWrapper
from ImageSaverLib.Errors import ResourceManipulatedException, FragmentMissingException
from ImageSaverLib.MetaDB.Errors import NotExistingException
from ImageSaverLib.MetaDB.MetaDB import MetaDBInterface
from ImageSaverLib.MetaDB.Types.Fragment import FragmentHash, Fragment, FragmentID, FragmentSize, FragmentPayloadSize
from ImageSaverLib.MetaDB.Types.FragmentResourceMapping import FragmentOffset
from ImageSaverLib.MetaDB.Types.Resource import (ResourceWrappingType, ResourceCompressionType, ResourceSize,
                                                 ResourceHash, Resource, ResourcePayloadSize)
from ImageSaverLib.PendingObjectsController import PendingObjectsController
from ImageSaverLib.Storage.StorageInterface import StorageInterface


class FlushError(Exception):
    orig_error = None  # type: Exception


class FragmentCache(object):
    POLICY_PASS = 1
    # POLICY_PASS will only build percentage filled resources, except during forceflush, then also not fully filled
    # resources get produced.
    # POLICY_PASS will mainly upload percentage filled resources, resulting in less traffic
    POLICY_FILL = 2
    # POLICY_FILL will mainly upload percentage filled resource, if fragments are remaining a filling run starts after
    # the percentage filling, only during forceflush not fully filled resources get uploaded
    # POLICY_FILL will mainly upload percentage filled resources. depending on the given fragment sizes and the
    # 'appendability' of resources, traffic might increase
    POLICY_FILL_ALWAYS = 3

    # POLICY_FILL_ALWAYS only appends fragments to resources. if fragments cannot get appended to resources, percentage
    # filled resources are built. only during forceflush not fully filled resources get produced
    # POLICY_FILL_ALWAYS will mainly append fragments to resources, which results in lots of traffic and garbage
    # collectible resources.
    def __init__(self, meta, storage, expected_fragmentsize, resource_wrap_type, resource_compress_type, resource_size,
                 pending_objects_controller, auto_wrapper, auto_compresser, resource_minimum_filllevel=0.5, auto_delete_resource=False, debug=False):
        # type: (MetaDBInterface, StorageInterface, int, Union[ResourceWrappingType, WrappingType], Union[ResourceCompressionType, CompressionType], ResourceSize, PendingObjectsController, AutoWrapper, AutoCompressor, float, bool, bool) -> None
        # super().__init__(meta, storage, expected_fragmentsize, resource_wrap_type, resource_compress_type,
        #                  resource_size, pending_objects_controller,
        #                  resource_minimum_filllevel, auto_delete_resource=auto_delete_resource)
        """
                Upload cache for fragments, caches given fragments. Packs as much fragments together to one resource.
                Optionally appends fragments to small resources.

                :param meta: cache_meta to use.
                :param storage: storage to use (RamStorageCache recommended if FILL-Policy is selected)
                :param expected_fragmentsize: the size of fragments which will get added. Used to calculate a optimal Cache
                size. Only needed, if cachesize is not given
                :param resource_wrap_type: specifies, how resources are wrapped
                :param resource_compress_type: specifies, how resources are compressed
                :param resource_size: specifies the maximum payload size of an fragment, Fragments get bigger after
                encapsulation is applied
                # :param cache_size: how many fragments are cached, should be at least enough to build one resource.
                # otherwise a lot of small resources are created or resources are constantly re-downloaded, extended and
                # reuploaded
                :param resource_minimum_filllevel: how much space of a resource should be at least used, before it is considered
                a 'upload worthy' resource
                """
        assert expected_fragmentsize <= resource_size, repr((expected_fragmentsize, resource_size))
        assert 0 < resource_minimum_filllevel <= 1.0
        self.resource_size = resource_size
        self.policy = self.POLICY_PASS
        self.auto_delete_resource = auto_delete_resource
        self.resource_minimum_filllevel = resource_minimum_filllevel

        self.resource_wrap_type = resource_wrap_type
        self.resource_compress_type = resource_compress_type
        self.auto_wrapper = auto_wrapper
        self.auto_compresser = auto_compresser
        self.meta = meta
        self.storage = storage
        self.pending_objects = pending_objects_controller

        self.fragment_cache = OrderedDict()  # type: OrderedDict[FragmentHash, Tuple[bytes, Fragment]]
        self.cache_total_fragmentsize = 0
        self._in_context = 0
        self.cache_last_downloaded_resource = True
        # self.last_downloaded_resource = None  # type: Optional[Tuple[ResourceHash, bytes]]
        self.last_downloaded_resource_hash = None  # type: Optional[ResourceHash]
        self.last_downloaded_resource_fragments = {}  # type: Dict[FragmentHash, bytes]
        self.debug = debug
        self.resource_reuse_blacklist = set()  # type: Set[ResourceHash]
        self._mutex = RLock()
        self._on_upload = None  # type: Optional[Callable[[ResourceSize, int], None]]
        self._on_download = None  # type: Optional[Callable[[ResourceSize, int], None]]
        self.upload_on_exception = False
        self.resource_packer = ResourcePacker()

    def __enter__(self):
        with self._mutex:
            self._in_context += 1
            return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        with self._mutex:
            # print("fcache", self._in_context, self.upload_on_exception, exc_type, exc_val, exc_tb)
            self._in_context -= 1
            if exc_type:
                if self.upload_on_exception:
                    # noinspection PyBroadException
                    try:
                        # print('fcache flush 1')
                        self._flush(totalflush=True)
                    except Exception:
                        pass
                return False
            else:
                if self._in_context == 0:
                    # print('fcache flush 2')
                    self._flush(totalflush=True)
            return self

    @property
    def lock(self):
        # type: () -> RLock
        return self._mutex

    @property
    def onUpload(self):
        return self._on_upload

    @onUpload.setter
    def onUpload(self, value):
        # type: (Callable[[ResourceSize, int], None]) -> None
        self._on_upload = value

    @property
    def onDownload(self):
        return self._on_download

    @onDownload.setter
    def onDownload(self, value):
        # type: (Callable[[Resource], None]) -> None
        self._on_download = value

    def removeFragment(self, fragment_hash):
        # type: (FragmentHash) -> None
        with self._mutex:
            if fragment_hash in self.fragment_cache:
                _, fragment = self.fragment_cache.pop(fragment_hash)
                self.cache_total_fragmentsize -= fragment.fragment_size

    def addFragmentData(self, fragment_data, fragment_hash, fragment_payload_size):
        # type: (bytes, FragmentHash, FragmentPayloadSize) -> Fragment
        with self._mutex:
            # print(threading.current_thread().name, 'pushes', len(fragment_data), 'bytes')
            fragment_size = FragmentSize(len(fragment_data))
            if fragment_hash in self.fragment_cache:
                return self.fragment_cache[fragment_hash][1]
            else:
                # fragment = self.meta.makeFragment(fragment_hash, fragment_size, fragment_payload_size)
                fragment = Fragment(fragment_hash, fragment_size, fragment_payload_size,
                                    # pending=FragmentPendingFlag(False)
                                    )
                self.addFragment(fragment_data, fragment)
                # print("???", len(self.fragment_cache))
                return fragment

    def addFragment(self, fragment_data, fragment, readd=False):
        # type: (bytes, Fragment, bool) -> None
        with self._mutex:
            # print("add fragment", len(fragment_data), self.fragmentsize, fragment.fragment_size)
            # assert len(self.fragment_cache) < self.resource_size
            assert self.cache_total_fragmentsize == sum((f.fragment_size for _, f in self.fragment_cache.values())), repr([self.cache_total_fragmentsize, sum((f.fragment_size for _, f in self.fragment_cache.values()))])
            meta_has_fragment = self.meta.hasFragmentByPayloadHash(fragment.fragment_hash)
            # try:
            #     meta_has_fragment_or_is_pending = self.meta.getFragmentByPayloadHash(fragment.fragment_hash).fragment_pending
            #     meta_has_fragment = True
            # except NotExistingException:
            #     meta_has_fragment_or_is_pending = True
            #     meta_has_fragment = False
            if not fragment.fragment_size == len(fragment_data):
                raise ValueError("size of given fragment_data and fragment.fragment_size differ")
            if not fragment.fragment_size <= self.resource_size:
                raise ValueError(
                    'given fragment is too big to fit into a resource, max is ' + str(self.resource_size) + ", got " + str(
                        fragment.fragment_size))
            if fragment.fragment_hash in self.fragment_cache:  # fragment already in cache, gets uploaded to storage with next flush
                return
            # elif not readd and not meta_has_fragment_or_is_pending and meta_has_fragment:  # fragment already uploaded to storage, mapping to resource exists
            elif not readd and meta_has_fragment:  # fragment already uploaded to storage, mapping to resource exists
                # print('elif not meta_has_fragment_or_is_pending')
                return
            # elif self.cache_total_fragmentsize > self.resource_size or (
            #         self.cache_total_fragmentsize / self.resource_size) >= self.resource_minimum_filllevel:
            elif self.cache_total_fragmentsize >= self.resource_size:
                self._flush()
            self.fragment_cache[fragment.fragment_hash] = (fragment_data, fragment)
            self.cache_total_fragmentsize += fragment.fragment_size
            self.pending_objects.addFragment(fragment)
            assert self.cache_total_fragmentsize == sum((f.fragment_size for _, f in self.fragment_cache.values())), repr([self.cache_total_fragmentsize, sum((f.fragment_size for _, f in self.fragment_cache.values()))])

    def loadFragment(self, fragment):
        # type: (Fragment) -> bytes
        with self._mutex:
            try:
                return self.fragment_cache[fragment.fragment_hash][0]
            except KeyError:
                pass
            if self.cache_last_downloaded_resource:
                if fragment.fragment_hash in self.last_downloaded_resource_fragments:
                    # print("reusing cached fragment from resource")
                    fragment_payload = self.last_downloaded_resource_fragments[fragment.fragment_hash]
                else:
                    try:
                        if fragment.fragment_id is None:
                            fragment_id = self.meta.getFragmentByPayloadHash(fragment.fragment_hash).fragment_id
                        else:
                            fragment_id = fragment.fragment_id
                        resource, fragment_offset = self.meta.getResourceOffsetForFragment(fragment_id)
                    except NotExistingException:
                        raise FragmentMissingException(
                            "No fragment offsets found for Fragment with id " + repr(fragment.fragment_id))
                    # print("downloading resource and caching fragments", resource.resource_hash.hex())
                    self.last_downloaded_resource_fragments.clear()
                    resource_payload = self.loadResource(resource)
                    self.last_downloaded_resource_hash = resource.resource_hash
                    fragments_offsets = self.meta.getFragmentsWithOffsetOnResource(resource.resource_id)
                    for _fragment, offset in fragments_offsets:
                        fragment_payload = resource_payload[offset:offset+_fragment.fragment_size]
                        # assert len(fragment_payload) == _fragment.fragment_size
                        # assert hashlib.sha256(fragment_payload).digest() == _fragment.fragment_hash
                        self.last_downloaded_resource_fragments[_fragment.fragment_hash] = fragment_payload
                    fragment_payload = self.last_downloaded_resource_fragments[fragment.fragment_hash]
            else:
                try:
                    if fragment.fragment_id is None:
                        fragment_id = self.meta.getFragmentByPayloadHash(fragment.fragment_hash).fragment_id
                    else:
                        fragment_id = fragment.fragment_id
                    resource, fragment_offset = self.meta.getResourceOffsetForFragment(fragment_id)
                except NotExistingException:
                    raise FragmentMissingException(
                        "No fragment offsets found for Fragment with id " + repr(fragment.fragment_id))
                # print("skipping caching of resource fragments")
                resource_payload = self.loadResource(resource)
                fragment_payload = resource_payload[fragment_offset:fragment_offset + fragment.fragment_size]
            # try:
            #     resource, fragment_offset = self.meta.getResourceOffsetForFragment(fragment.fragment_id)
            # except NotExistingException:
            #     raise FragmentMissingException(
            #         "No fragment offsets found for Fragment with id " + repr(fragment.fragment_id))
            # if self.last_downloaded_resource and self.last_downloaded_resource[0] == resource.resource_hash:
            #     fragment_payload = self.last_downloaded_resource[1][
            #                        fragment_offset:fragment_offset + fragment.fragment_size]
            # else:
            #     resource_payload = self.loadResource(resource)
            #     self.last_downloaded_resource = (
            #         resource.resource_hash, resource_payload)  # type: Optional[Tuple[ResourceHash, bytes]]
            #     fragment_payload = resource_payload[fragment_offset:fragment_offset + fragment.fragment_size]
            assert len(fragment_payload) == fragment.fragment_size
            return fragment_payload

    def flush(self, force=False):
        """
        tries to empty the built up block cache.
        :raises FlushError: flushing produced an error, you should revert your fragments
        """
        with self._mutex:
            if self._in_context > 0 and not force:
                if self.debug:
                    pass
                    # print("cannot flush manually inside a context!", threading.current_thread().name, 'stacked context', self._in_context)
            # if self.cache_total_fragmentsize > self.resource_size or (
            #         self.cache_total_fragmentsize / self.resource_size) >= self.resource_minimum_filllevel or force:
            elif force:
                self._flush(totalflush=True)
            elif self.cache_total_fragmentsize >= self.resource_size or force:
                self._flush(totalflush=False)
            # region add compounds to meta which are 'finished' based on PendingObjectsControlelr
            non_pending_fragment_sequences = self.pending_objects.popNonPendingFragmentSequences()
            if non_pending_fragment_sequences:
                if self.debug:
                    print("persisting", len(non_pending_fragment_sequences), "compounds with a total of", sum((len(fpi) for fpi in non_pending_fragment_sequences.values())), "fragment mappings in meta")
                for compound, fragment_payload_index in non_pending_fragment_sequences.items():
                    assert compound.compound_id is None
                    assert all((f.fragment_id is None for f, _ in fragment_payload_index))
                    # print('persisiting', compound.compound_name)
                    self.meta.addOverwriteCompoundAndMapFragments(compound, fragment_payload_index)
            # endregion

    def _flush(self, totalflush=False):
        with self._mutex:
            assert self.cache_total_fragmentsize == sum((f.fragment_size for _, f in self.fragment_cache.values())), repr([self.cache_total_fragmentsize, sum((f.fragment_size for _, f in self.fragment_cache.values()))])
            if len(self.fragment_cache) == 0:
                # cache is empty, skipping everything
                if self.debug:
                    print("cache is empty, skipping flushing, totalflush="+str(totalflush))
                return
            # if self.debug:
            #     print("mid flushing possible, totalflush="+str(totalflush))
                # print(hash(tuple(self.fragment_cache.keys())))
                # print("max fragment id", max((f.fragment_id for _, f in self.fragment_cache.values())))
                # traceback.print_stack()
            if self.policy == self.POLICY_PASS:
                # if self.debug:
                #     print("with policy pass")
                self._flush_percentage_filled(totalflush)
            elif self.policy == self.POLICY_FILL:
                # if self.debug:
                #     print("with policy fill")
                self._flush_percentage_filled(totalflush)
                self._flush_resource_appending()
            elif self.policy == self.POLICY_FILL_ALWAYS:
                # if self.debug:
                #     print("with policy fill_always")
                self._flush_resource_appending()
            else:
                raise NotImplementedError

            # after emptying out the cache (few fragments might remain, cannot build full resource from it),
            # check if we should totalflush the cache
            if totalflush:
                # if self.debug:
                #     print("totalflush flushing")
                # depending on policy, simply upload remaining or fill other resources.
                if self.policy == self.POLICY_PASS:
                    # if self.debug:
                    #     print("with pass")
                    # policy says, to simply upload remaining blocks as one resource, ignore storage wasting
                    self._flush_percentage_filled(empty=True)
                elif self.policy == self.POLICY_FILL:
                    # if self.debug:
                    #     print("with policy fill")
                    self._flush_percentage_filled()
                    self._flush_resource_appending(empty=True)
                elif self.policy == self.POLICY_FILL_ALWAYS:
                    # if self.debug:
                    #     print("with policy fill_always")
                    self._flush_resource_appending(empty=True)
                assert len(self.fragment_cache) == 0, repr(len(self.fragment_cache)) + ' ' + repr(
                    {h: f for h, (_, f) in self.fragment_cache.items()})
            # region add compounds to meta which are 'finished' based on PendingObjectsControlelr
            non_pending_fragment_sequences = self.pending_objects.popNonPendingFragmentSequences()
            if non_pending_fragment_sequences:
                if self.debug:
                    print("persisting", len(non_pending_fragment_sequences), "compounds with a total of",
                          sum((len(fpi) for fpi in non_pending_fragment_sequences.values())), "fragment mappings in meta")
                for compound, fragment_payload_index in non_pending_fragment_sequences.items():
                    assert compound.compound_id is None
                    assert all((f.fragment_id is None for f, _ in fragment_payload_index))
                    self.meta.addOverwriteCompoundAndMapFragments(compound, fragment_payload_index)
            # endregion

    def _upload(self, fragments_data, update=None, fragments_count=None):
        # type: (Union[Iterable[bytes], bytes], Optional[Resource], Optional[int]) -> Resource
        """
        Packs given blocks into one resource and uploads it, ignores resource size limitations
        """
        with self._mutex:
            # encapsulate resource
            # upload resource
            # add blocks+resource to cache_meta after successful upload
            if type(fragments_data) in (bytes, bytearray):
                resource_data = fragments_data
                if fragments_count is None:
                    fragments_count = 1
            else:
                fragments_data = list(fragments_data)
                resource_data = bytes().join((d for d in fragments_data))
                fragments_count = len(fragments_data)
            # print('???', hashlib.sha256(resource_data).hexdigest())
            resource_payloadsize = ResourcePayloadSize(len(resource_data))
            resource_data = encapsulate(self.auto_compresser, self.auto_wrapper, self.resource_compress_type,
                                        self.resource_wrap_type,
                                        resource_data)
            resource_hash = ResourceHash(hashlib.sha256(resource_data).digest())
            resource_size = ResourceSize(len(resource_data))
            try:
                resource = self.meta.getResourceForResourceHash(resource_hash)
                resource_name = resource.resource_name
            except NotExistingException:
                # print("calling _on_upload", bool(self._on_upload))
                if self._on_upload:
                    self._on_upload(resource_size, fragments_count)
                resource_name = self.storage.saveResource(resource_data, resource_hash, resource_size)
                if self.debug:
                    print("created resource ("+humanfriendly.format_size(resource_size)+") with name:", resource_name)
            resource = self.meta.makeResource(resource_name, resource_size, resource_payloadsize, resource_hash,
                                              self.resource_wrap_type,
                                              self.resource_compress_type)
            if update:
                self.meta.moveFragmentMappings(update.resource_id, resource.resource_id)
                if self.auto_delete_resource and update:
                    self.storage.deleteResource(update.resource_name)
            return resource

    def loadResource(self, resource):
        # type: (Resource) -> bytes
        """
        helper, downlaods, dewraps and decompresses resource from storage
        """
        with self._mutex:
            if self._on_download:
                self._on_download(resource)
            resource_data = self.storage.loadRessource(resource.resource_name)
            resource_size = ResourceSize(len(resource_data))
            if resource_size != resource.resource_size:
                raise ResourceManipulatedException("resource size is not the expected one")
            resource_hash = ResourceHash(hashlib.sha256(resource_data).digest())
            if resource_hash != resource.resource_hash:
                raise ResourceManipulatedException("resource hash is not the expected one")
            payload = decapsulate(self.auto_compresser, self.auto_wrapper, resource.compression_type, resource.wrapping_type,
                                  resource_data)
            if len(payload) != resource.resource_payloadsize:
                raise ResourceManipulatedException("decapsulated resource has incorrect size, expected " + str(
                    resource.resource_payloadsize) + ", got " + str(len(payload)))
            return payload

    def loadFragmentsOfResource(self, resource):
        # type: (Resource) -> List[Tuple[Fragment, bytes]]
        with self._mutex:
            return_list = []
            resource_payload = self.loadResource(resource)
            fragments_with_offsets = self.meta.getFragmentsWithOffsetOnResource(resource.resource_id)
            for fragment, offset in fragments_with_offsets:
                return_list.append((fragment, resource_payload[offset:offset + fragment.fragment_size]))
            return return_list

    def _upload_and_map_fragments(self, fragment_hashes, update=None):
        # type: (List[FragmentHash], Optional[Resource]) -> None
        with self._mutex:
            assert len(fragment_hashes) == len(set(fragment_hashes))
            assert self.cache_total_fragmentsize == sum((f.fragment_size for _, f in self.fragment_cache.values())), repr([self.cache_total_fragmentsize, sum((f.fragment_size for _, f in self.fragment_cache.values()))])
            if len(fragment_hashes) == 0:
                return
            fragments_id_offset = []  # type: List[Tuple[FragmentID, FragmentOffset]]
            fragments_buffer_size = 0
            assert self.cache_total_fragmentsize >= 0
            assert self.cache_total_fragmentsize == sum((f.fragment_size for _, f in self.fragment_cache.values())), repr([self.cache_total_fragmentsize, sum((f.fragment_size for _, f in self.fragment_cache.values()))])
            resource = self._upload((self.fragment_cache.get(h)[0] for h in fragment_hashes), update=update)
            assert resource.resource_id is not None
            if self.debug:
                print("containing", len(fragment_hashes), "fragments")
            # region create a list with fragments and offsets for resource and map it to a resource
            # this updates the meta: creating fragments and create their mappings to a resource
            fragments_offset = []  # type: List[Tuple[Fragment, FragmentOffset]]
            for index, fragment_hash in enumerate(fragment_hashes):
                fragment_data, fragment = self.fragment_cache.get(fragment_hash)
                assert len(fragment_data) == fragment.fragment_size
                fragments_offset.append((fragment, FragmentOffset(fragments_buffer_size)))
                fragments_buffer_size += fragment.fragment_size
                assert self.cache_total_fragmentsize-fragments_buffer_size >= 0, \
                    repr([self.cache_total_fragmentsize,
                          fragments_buffer_size,
                          self.cache_total_fragmentsize-fragments_buffer_size,
                          index, len(fragment_hashes)])
            fragments_offset = self.meta.makeAndMapFragmentsToResource(resource.resource_id, fragments_offset)
            # endregion
            assert self.cache_total_fragmentsize == sum((f.fragment_size for _, f in self.fragment_cache.values())), repr([self.cache_total_fragmentsize, sum((f.fragment_size for _, f in self.fragment_cache.values()))])
            # region remove fragments+data from cache
            for fragment_hash in fragment_hashes:
                _, fragment = self.fragment_cache.get(fragment_hash)
                self.pending_objects.removeFragment(fragment)
                self.fragment_cache.pop(fragment_hash)
                self.cache_total_fragmentsize -= fragment.fragment_size
            # endregion
            # region add compounds to meta which are 'finished' based on PendingObjectsControlelr
            non_pending_fragment_sequences = self.pending_objects.popNonPendingFragmentSequences()
            if non_pending_fragment_sequences:
                if self.debug:
                    print("persisting", len(non_pending_fragment_sequences), "compounds with a total of",
                          sum((len(fpi) for fpi in non_pending_fragment_sequences.values())), "fragment mappings in meta")
                for compound, fragment_payload_index in non_pending_fragment_sequences.items():
                    assert compound.compound_id is None
                    assert all((f.fragment_id is None for f, _ in fragment_payload_index))
                    self.meta.addOverwriteCompoundAndMapFragments(compound, fragment_payload_index)
            # endregion

            if self.debug:
                print("remaining fragments:", len(self.fragment_cache))
            assert self.cache_total_fragmentsize >= 0, repr([fragments_buffer_size, self.cache_total_fragmentsize])



    def _flush_percentage_filled(self, empty=False):
        """
        pack fragments in cache into percentage or fully filled resources if possible.
        if empty is true, the cache gets emptied completely
        """
        with self._mutex:
            # test02
            # print(list(self.fragment_cache.values()))
            # fragments_dict = {f: f.fragment_size for _, f in self.fragment_cache.values()}
            resources = self.resource_packer.getFragmentPackets(self.resource_size, (f for _, f in self.fragment_cache.values()))
            # if not empty:
            #     resources = self.resource_packer.checkPackagesReachMinimumFillLevel(resources, self.resource_size,
            #                                                             self.resource_minimum_filllevel,
            #                                                             not empty)
            # resources = sorted(resources, key=lambda r: sum(r.values()), reverse=True)
            if empty is False:
                if not self.resource_packer.checkPackagesReachMinimumFillLevel(resources, self.resource_size, self.resource_minimum_filllevel):
                    print("unable to flush fragment cache without forceful emptying, all fragment packets would not reach "
                          "the desired minimum resource fill level. "
                          "this can lead to a huge memory leak", file=sys.stderr)
                    return
                else:
                    resources = [resources[0]]
            for packed_fragments_size_dict in resources:
                fragment_package_list = sorted(packed_fragments_size_dict.keys(), key=lambda f: f.fragment_hash)
                self._upload_and_map_fragments([f.fragment_hash for f in fragment_package_list])

    def _flush_resource_appending(self, empty=False):
        """
        fetches smallest resource payload-wise and tries to append fragments to reach the percentage fill level
        will not flush fragments, that are not "appendable" because they are too big.
        """
        # if self.debug:
        #     print("_flush_resource_appending", empty)
        with self._mutex:
            abort_appending = False
            while not abort_appending:
                # print('loop', len(self.fragment_cache))
                smallest_resource = self.meta.getSmallestResource(ignore=self.resource_reuse_blacklist)
                # check if we have fragments with size smaller than append_max_resource_size

                if smallest_resource:
                    append_max_resource_size = self.resource_size - smallest_resource.resource_payloadsize
                    having_appendable_fragments = any(
                        (f.fragment_size <= append_max_resource_size for _, f in self.fragment_cache.values()))
                else:
                    append_max_resource_size = self.resource_size
                    having_appendable_fragments = False
                if having_appendable_fragments:
                    # print('having appedable fragments', len([f for _, f in self.fragment_cache.values() if f.fragment_size <= append_max_resource_size]))
                    fragments_dict = {f: f.fragment_size for _, f in self.fragment_cache.values()}
                    resources = binpacking.to_constant_volume(fragments_dict, append_max_resource_size,
                                                              upper_bound=append_max_resource_size + 1)
                    if not resources:
                        raise NotImplementedError
                    if len(resources) == 0 or len(resources[0]) == 0:
                        self._flush_percentage_filled(empty=empty)
                        abort_appending = True
                        continue

                    resources = sorted(resources, key=lambda r: sum(r.values()), reverse=True)
                    packed_fragments_size_dict = resources[0]
                    # print(resources)
                    resource_payload_size = sum(packed_fragments_size_dict.values())
                    uploadable_fragment_hashes = [f.fragment_hash for f in packed_fragments_size_dict.keys()]
                    assert resource_payload_size <= append_max_resource_size, repr(resource_payload_size) + ' ' + repr(
                        append_max_resource_size) + ' ' + repr(packed_fragments_size_dict)
                    # if (resource_payload_size+append_max_resource_size) / self.resource_size <= self.resource_minimum_filllevel:
                    #     abort_appending = True
                    #     # assert len(resources) == 1, repr(resources)
                    #     # print("but resulting resource payload size would be too small")
                    #     # if len(resources) == 1:
                    #     self._flush_percentage_filled(empty=empty)
                    #     # print('ONLYONCE'+str(len(self.fragment_cache)))
                    #     continue
                    resource_fragments = self.loadFragmentsOfResource(smallest_resource)
                    for fragment, data in resource_fragments:
                        # assert fragment.fragment_hash not in self.fragment_cache
                        if fragment.fragment_hash not in self.fragment_cache:
                            self.fragment_cache[fragment.fragment_hash] = (data, fragment)
                            self.cache_total_fragmentsize += len(data)
                            assert fragment.fragment_hash not in uploadable_fragment_hashes
                            uploadable_fragment_hashes.append(fragment.fragment_hash)
                            assert self.cache_total_fragmentsize == sum(
                                (f.fragment_size for _, f in self.fragment_cache.values())), repr(
                                [self.cache_total_fragmentsize,
                                 sum((f.fragment_size for _, f in self.fragment_cache.values()))])
                        else:
                            assert self.fragment_cache[fragment.fragment_hash][0] == data
                            assert self.fragment_cache[fragment.fragment_hash][1].fragment_hash == fragment.fragment_hash
                            assert self.fragment_cache[fragment.fragment_hash][1].fragment_size == fragment.fragment_size

                    # print('appending', len(packed_fragments_size_dict.keys()), "fragments to resource")
                    _old_len = len(self.fragment_cache)
                    self._upload_and_map_fragments(
                        uploadable_fragment_hashes,
                        # [f.fragment_hash for f in packed_fragments_size_dict.keys()] + [f.fragment_hash for f, _ in
                        #                                                                 resource_fragments],
                        update=smallest_resource)
                    assert len(self.fragment_cache) < _old_len
                else:
                    self._flush_percentage_filled(empty=empty)
                    abort_appending = True


class ResourcePacker(object):
    BIN_PACKING = 1
    FILLING = 2

    def __init__(self, packing_method=FILLING):
        # type: (int) -> None
        self.packing_method = packing_method

    def getFragmentPackets(self, resource_size, fragments):
        # type: (ResourceSize, Iterable[Fragment]) -> List[Dict[Fragment, FragmentSize]]
        # fragments_size_dict = {f:f.fragment_size for f in fragments}
        if self.packing_method == self.BIN_PACKING:
            packets = self._get_binpacked_resources(resource_size, fragments)
        elif self.packing_method == self.FILLING:
            packets = self._get_sequential_filled_resources(resource_size, fragments)
        else:
            raise NotImplementedError
        return sorted(packets, key=lambda packet: sum(packet.values()), reverse=True)

    def checkPackagesReachMinimumFillLevel(self, packets, resource_size, minimum_fill_level):
        # type: (List[Dict[Fragment, FragmentSize]], ResourceSize, float) -> bool
        if minimum_fill_level > 1.0:
            raise ValueError("fill level greater than 100% | 1.0")
        return any((sum(packet.values()) / resource_size >= minimum_fill_level for packet in packets))

    def _get_binpacked_resources(self, resource_size, fragments):
        # type: (ResourceSize, Iterable[Fragment]) -> List[Dict[Fragment, FragmentSize]]
        return binpacking.to_constant_volume({f:f.fragment_size for f in fragments}, resource_size, upper_bound=resource_size + 1)

    def _get_sequential_filled_resources(self, resource_size, fragments):
        # type: (ResourceSize, Iterable[Fragment]) -> List[Dict[Fragment, FragmentSize]]
        buckets = []
        new_bucket = OrderedDict()
        new_bucket_size = 0
        for fragment in fragments:
            if new_bucket_size + fragment.fragment_size > resource_size:
                buckets.append(new_bucket)
                new_bucket = OrderedDict()
                new_bucket_size = 0
            new_bucket[fragment] = fragment.fragment_size
            new_bucket_size += fragment.fragment_size
        if len(new_bucket):
            buckets.append(new_bucket)
        return buckets


class PackingError(Exception):
    pass
