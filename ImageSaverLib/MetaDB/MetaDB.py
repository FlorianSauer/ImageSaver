from abc import ABC, abstractmethod
from typing import List, Tuple, Optional, Iterable, Any

from ImageSaverLib.Helpers.SizedGenerator import SizedGenerator
from ImageSaverLib.MetaDB.Types.Compound import (Compound, CompoundName, CompoundID, CompoundType, CompoundHash,
                                                 CompoundSize, CompoundWrappingType, CompoundCompressionType,
                                                 CompoundVersion)
from ImageSaverLib.MetaDB.Types.CompoundFragmentMapping import SequenceIndex
from ImageSaverLib.MetaDB.Types.Fragment import Fragment, FragmentID, FragmentHash, FragmentSize, FragmentPayloadSize
# from ImageSaverLib2.MetaDB.Types.Payload import Payload, PayloadHash, PayloadSize, PayloadID
from ImageSaverLib.MetaDB.Types.FragmentResourceMapping import FragmentOffset
from ImageSaverLib.MetaDB.Types.Resource import (Resource, ResourceName, ResourceCompressionType, ResourceWrappingType,
                                                 ResourceHash,
                                                 ResourceSize, ResourceID, ResourcePayloadSize)


class MetaDBInterface(ABC):
    def __init__(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            raise
        return self

    @abstractmethod
    def getCompoundByName(self, compound_name, compound_version=CompoundVersion(None)):
        # type: (CompoundName, CompoundVersion) -> Compound
        pass

    @abstractmethod
    def getCompoundByHash(self, compound_hash, compound_version=CompoundVersion(None)):
        # type: (CompoundHash, CompoundVersion) -> Compound
        pass

    @abstractmethod
    def makeFragment(self, fragment_hash, fragment_size, fragment_payload_size):
        # type: (FragmentHash, FragmentSize, FragmentPayloadSize) -> Fragment
        pass

    @abstractmethod
    def hasFragmentByPayloadHash(self, fragment_hash):
        # type: (FragmentHash) -> bool
        pass

    @abstractmethod
    def getFragmentByPayloadHash(self, fragment_hash):
        # type: (FragmentHash) -> Fragment
        pass

    @abstractmethod
    def makeResource(self, resource_name, resource_size, resource_payloadsize, resource_hash, wrap_type, compress_type):
        # type: (ResourceName, ResourceSize, ResourcePayloadSize, ResourceHash, ResourceWrappingType, ResourceCompressionType) -> Resource
        pass

    # @abstractmethod
    # def makeFragmentResourceMapping(self, fragment_id, resource_id):
    #     # type: (FragmentID, ResourceID) -> FragmentResourceMapping
    #     pass

    # @abstractmethod
    # def makePayload(self, payload_hash, payload_size):
    #     # type: (PayloadHash, PayloadSize) -> Payload
    #     pass

    @abstractmethod
    def setFragmentsMappingForCompound(self, compound_id, fragment_sequence_index):
        # type: (CompoundID, List[Tuple[FragmentID, SequenceIndex]]) -> None
        pass

    @abstractmethod
    def makeCompound(self, name, compound_type, compound_hash, compound_size, wrapping_type, compression_type):
        # type: (CompoundName, CompoundType, CompoundHash, CompoundSize, CompoundWrappingType, CompoundCompressionType) -> Compound
        pass

    @abstractmethod
    def makeSnapshottedCompound(self, compound):
        # type: (Compound) -> Compound
        pass

    @abstractmethod
    def updateCompound(self, name, compound_type, compound_hash, compound_size, wrapping_type, compression_type):
        # type: (CompoundName, CompoundType, CompoundHash, CompoundSize, CompoundWrappingType, CompoundCompressionType) -> Compound
        """
        Get compound by name and update all other fields
        """
        pass

    # @abstractmethod
    # def setPayloadForCompound(self, compound_id, payload_id):
    #     # type: (CompoundID, PayloadID) -> None
    #     pass

    @abstractmethod
    def hasCompoundWithName(self, name, version=CompoundVersion(None)):
        # type: (CompoundName, CompoundVersion) -> bool
        pass

    @abstractmethod
    def hasCompoundWithHash(self, compound_hash, compound_version=CompoundVersion(None)):
        # type: (CompoundHash, CompoundVersion) -> bool
        pass

    # @abstractmethod
    # def getPayloadForCompound(self, compound_id):
    #     # type: (CompoundID) -> Optional[Payload]
    #     pass

    @abstractmethod
    def getSequenceIndexSortedFragmentsForCompound(self, compound_id):
        # type: (CompoundID) -> SizedGenerator[Tuple[SequenceIndex, Fragment]]
        pass

    @abstractmethod
    def getFragmentHashesNeededForCompound(self, compound_id):
        # type: (CompoundID) -> SizedGenerator[FragmentHash]
        pass

    # @abstractmethod
    # def hasPayloadForCompound(self, compound_id):
    #     # type: (CompoundID) -> bool
    #     pass

    @abstractmethod
    def getAllCompounds(self, type_filter=None, order_alphabetically=False, starting_with=None, ending_with=None, slash_count=None, min_size=None, include_snapshots=False):
        # type: (Optional[CompoundType], bool, Optional[str], Optional[str], Optional[int], Optional[int], bool) -> SizedGenerator[Compound]
        pass

    @abstractmethod
    def getAllCompoundsSizeSum(self, type_filter=None, starting_with=None, ending_with=None, slash_count=None, min_size=None):
        # type: (Optional[CompoundType], Optional[str], Optional[str], Optional[int], Optional[int]) -> int
        pass

    @abstractmethod
    def getAllCompoundNames(self):
        # type: () -> SizedGenerator[CompoundName, Any, None]
        pass

    @abstractmethod
    def getAllCompoundNamesWithVersion(self, include_snapshots=False):
        # type: (bool) -> SizedGenerator[Tuple[CompoundName, CompoundVersion], Any, None]
        pass

    @abstractmethod
    def getTotalCompoundSize(self):
        # type: () -> int
        pass

    @abstractmethod
    def getTotalCompoundCount(self, with_type=None):
        # type: (Optional[CompoundType]) -> int
        pass

    @abstractmethod
    def getSnapshotCount(self, with_type=None):
        # type: (Optional[CompoundType]) -> int
        pass

    @abstractmethod
    def getUniqueCompoundSize(self):
        # type: () -> int
        pass

    @abstractmethod
    def getUniqueCompoundCount(self):
        # type: () -> int
        pass

    @abstractmethod
    def getTotalFragmentSize(self):
        # type: () -> int
        pass

    @abstractmethod
    def getTotalFragmentCount(self):
        # type: () -> int
        pass

    @abstractmethod
    def getTotalResourceSize(self):
        # type: () -> int
        pass

    @abstractmethod
    def getTotalResourceCount(self):
        # type: () -> int
        pass

    @abstractmethod
    def removeCompound(self, compound_id):
        # type: (CompoundID) -> None
        pass

    @abstractmethod
    def renameCompound(self, old_name, new_name):
        # type: (CompoundName, CompoundName) -> None
        pass

    @abstractmethod
    def renameResource(self, old_resource_name, new_resource_name):
        # type: (ResourceName, ResourceName) -> None
        pass

    @abstractmethod
    def massRenameResource(self, old_new_resource_name_pairs, skip_unknown=False):
        # type: (List[Tuple[ResourceName, ResourceName]], bool) -> None
        pass

    @abstractmethod
    def collectGarbage(self, keep_fragments, keep_resources):
        # type: (bool, bool) -> None
        pass

    @abstractmethod
    def getUnneededFragments(self):
        # type: () -> SizedGenerator[Fragment]
        pass

    @abstractmethod
    def getUnneededFragmentHashes(self):
        # type: () -> SizedGenerator[FragmentHash]
        pass

    @abstractmethod
    def deleteResourceByID(self, resource_id):
        # type: (ResourceID) -> None
        pass

    @abstractmethod
    def deleteResourceByName(self, resource_name):
        # type: (ResourceName) -> None
        pass

    # @abstractmethod
    # def getPayloadByID(self, payload_id):
    #     # type: (PayloadID) -> Payload
    #     pass

    @abstractmethod
    def getResourceForFragment(self, fragment_id):
        # type: (FragmentID) -> Resource
        pass

    @abstractmethod
    def getResourceOffsetForFragment(self, fragment_id):
        # type: (FragmentID) -> Tuple[Resource, FragmentOffset]
        pass

    @abstractmethod
    def truncateAllCompounds(self):
        # type: () -> None
        pass

    @abstractmethod
    def getDuplicateFragmentsCount(self):
        # type: () -> int
        pass

    @abstractmethod
    def getSavedBytesByDuplicateFragments(self):
        # type: () -> int
        pass

    @abstractmethod
    def getMultipleUsedCompoundsCount(self, compound_type=None):
        # type: (Optional[CompoundType]) -> int
        pass

    @abstractmethod
    def getSavedBytesByMultipleUsedCompounds(self):
        # type: () -> int
        pass

    @abstractmethod
    def getAllResourceNames(self):
        # type: () -> SizedGenerator[ResourceName]
        pass

    @abstractmethod
    def getAllResources(self):
        # type: () -> SizedGenerator[Resource]
        pass

    @abstractmethod
    def getAllResourcesSizeSorted(self):
        # type: () -> SizedGenerator[Resource]
        pass

    @abstractmethod
    def getResourceNameForResourceHash(self, resource_hash):
        # type: (ResourceHash) -> ResourceName
        pass

    @abstractmethod
    def getResourceForResourceHash(self, resource_hash):
        # type: (ResourceHash) -> Resource
        pass

    @abstractmethod
    def getFragmentByID(self, fragment_id):
        # type: (FragmentID) -> Fragment
        pass

    @abstractmethod
    def hasFragmentResourceMappingForFragment(self, fragment_id):
        # type: (FragmentID) -> bool
        pass

    @abstractmethod
    def makeFragmentResourceMapping(self, fragment_id, resource_id, fragment_offset):
        # type: (FragmentID, ResourceID, FragmentOffset) -> None
        pass

    @abstractmethod
    def makeMultipleFragmentResourceMapping(self, resource_id, fragment_id_fragment_offset):
        # type: (ResourceID, List[Tuple[FragmentID, FragmentOffset]]) -> None
        pass

    @abstractmethod
    def getSmallestResource(self, ignore=None):
        # type: (Optional[Iterable[ResourceHash]]) -> Resource
        """
        returns smallest saved resource with at least one referenced and saved fragment
        """
        pass

    @abstractmethod
    def updateResource(self, resource_id, resource_name, resource_size, resource_payloadsize, resource_hash,
                       resource_wrap_type,
                       resource_compress_type):
        # type: (ResourceID, ResourceName, ResourceSize, ResourcePayloadSize, ResourceHash, ResourceWrappingType, ResourceCompressionType) -> Resource
        pass

    @abstractmethod
    def getUnreferencedFragments(self):
        # type: () -> SizedGenerator[Fragment]
        """
        Return list of Fragments, which are not referenced by any Compound
        """
        pass

    @abstractmethod
    def deleteFragments(self, unreferenced_fragments):
        # type: (Iterable[Fragment]) -> None
        pass

    @abstractmethod
    def getUnreferencedResources(self):
        # type: () -> SizedGenerator[Resource]
        pass

    @abstractmethod
    def removeCompoundByName(self, compoundname, keep_snapshots=False):
        # type: (CompoundName, bool) -> None
        pass

    def getResourceWithReferencedFragmentSize(self):
        # type: () -> SizedGenerator[Tuple[Resource, FragmentSize]]
        pass

    def getFragmentsWithOffsetOnResource(self, resource_id):
        # type: (ResourceID) -> SizedGenerator[Tuple[Fragment, FragmentOffset]]
        pass

    @abstractmethod
    def moveFragmentMappings(self, old_resource, new_resource):
        # type: (ResourceID, ResourceID) -> None
        pass

    @abstractmethod
    def getAllFragments(self):
        # type: () -> SizedGenerator[Fragment]
        pass

    def deleteUnreferencedFragments(self):
        # type: () -> None
        pass

    def getResourceByResourceName(self, resource_name):
        # type: (ResourceName) -> Resource
        pass

    # @classmethod
    # def orderCompoundResourceDependencies(cls, compound_resource_ids_dependencies):
    #     # type: (Dict[CompoundName, List[ResourceID]]) -> List[CompoundName]
    #     """
    #     Orderes and rsstructurizes the Resource Dependencies of Compounds, so Compounds with the same or simmilar
    #     Resource set are downloaded after each other.
    #     """
    #     order = []
    #     for name in compound_resource_ids_dependencies:
    #         print("ordering", name)
    #         inserted = False
    #         for index, ordered_name in enumerate(order):
    #             print('comparing', name, 'with ordered', ordered_name)
    #             if index > 0:
    #                 commonness_name_with_previous_current = len(set(compound_resource_ids_dependencies[name]).intersection(set(compound_resource_ids_dependencies[order[index - 1]])))
    #                 commonness_current_with_previous_current = len(
    #                     set(compound_resource_ids_dependencies[ordered_name]).intersection(set(compound_resource_ids_dependencies[order[index - 1]])))
    #                 if commonness_name_with_previous_current > commonness_current_with_previous_current:
    #                     print('current order', order + [name])
    #                     print(name, 'has more ids with', order[index - 1], 'in common than', ordered_name)
    #                     print(commonness_name_with_previous_current, '>', commonness_current_with_previous_current)
    #                     order.insert(index, name)
    #                     inserted = True
    #                     break
    #         if not inserted:
    #             order.append(name)
    #     return order
    #
    # @classmethod
    # def _getResourceIDs(cls, compound_resource_ids_dependencies, compound_name):
    @abstractmethod
    def getAllFragmentsWithNoResourceLink(self):
        # type: () -> SizedGenerator[Fragment]
        pass

    @abstractmethod
    def getAllCompoundsWithNoFragmentLink(self):
        # type: () -> SizedGenerator[Compound]
        """
        excludes compounds with size 0, because those compounds do not have any fragment naturally
        """
        pass

    @abstractmethod
    def getCompoundByHashWithFragmentLinks(self, compound_hash):
        # type: (CompoundHash) -> Compound
        pass

    def getAllFragmentsSortedByCompoundUsage(self):
        # type: () -> SizedGenerator[Tuple[CompoundID, SequenceIndex, Fragment]]
        pass

    @abstractmethod
    def makeAndMapFragmentsToResource(self, resource_id, fragments_offset):
        # type: (ResourceID, List[Tuple[Fragment, FragmentOffset]]) -> List[Tuple[Fragment, FragmentOffset]]
        pass

    @abstractmethod
    def addOverwriteCompoundAndMapFragments(self, compound, fragment_payload_index):
        # type: (Compound, List[Tuple[Fragment, SequenceIndex]]) -> Compound
        pass

    @abstractmethod
    def getSnapshotsOfCompound(self, compound_name, min_version=None, max_version=None, include_live_version=False):
        # type: (CompoundName, Optional[int], Optional[int], bool) -> SizedGenerator[Compound]
        """

        :param min_version: Compound.compound_version >= min_version
        :param max_version: Compound.compound_version &lt= min_version
        """
        pass
