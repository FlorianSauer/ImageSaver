from threading import RLock
from typing import List, Dict, Tuple, Optional

from ImageSaverLib4.MetaDB.Types.Compound import CompoundName
from ImageSaverLib4.MetaDB.Types.CompoundFragmentMapping import SequenceIndex
from ImageSaverLib4.MetaDB.Types.Fragment import FragmentHash
from .MetaDB.Types.Compound import Compound
from .MetaDB.Types.Fragment import Fragment


class PendingObjectsController(object):
    def __init__(self):
        self._mutex = RLock()
        self.compound_fragment_map = {}  # type: Dict[Compound, List[Fragment]]
        self.compound_fragment_sequence_map = {}  # type: Dict[Compound, List[Tuple[Fragment, SequenceIndex]]]
        self.pending_fragments = {}  # type: Dict[FragmentHash, Fragment]

    def addFragment(self, fragment):
        # type: (Fragment) -> None
        with self._mutex:
            if fragment.fragment_hash not in self.pending_fragments:
                self.pending_fragments[fragment.fragment_hash] = fragment
        # self.pending_fragments.append(fragment)

    def addCompound(self, compound, needed_fragments, fragment_sequence):
        # type: (Compound, List[Fragment], List[Tuple[Fragment, SequenceIndex]]) -> None
        with self._mutex:
            reservable_fragments = []
            for f in needed_fragments:
                if f.fragment_hash in self.pending_fragments:
                    reservable_fragments.append(f)
            if compound in self.compound_fragment_map:
                raise Exception('compound double pending '+repr(compound))
            if len(reservable_fragments) > 0:
                self.compound_fragment_map[compound] = reservable_fragments
            self.compound_fragment_sequence_map[compound] = fragment_sequence
            # else:
            #     print("addCompound: skipping, no reservable fragments")

    def removeFragment(self, fragment):
        # type: (Fragment) -> None
        with self._mutex:
            for k in self.compound_fragment_map:
                try:
                    fragment_list = self.compound_fragment_map[k]
                except ValueError:
                    continue
                self.compound_fragment_map[k] = [f for f in fragment_list if f.fragment_hash != fragment.fragment_hash]
                # for f in list(fragment_list):
                #     if f.fragment_hash == fragment.fragment_hash:
                #         fragment_list.remove(f)
            try:
                self.pending_fragments.pop(fragment.fragment_hash)
            except KeyError:
                pass
            for k in list(self.compound_fragment_map.keys()):
                if len(self.compound_fragment_map[k]) == 0:
                    self.compound_fragment_map.pop(k)
                    # print("removeFragment: removing pending compound", k.compound_name)

    def removeCompound(self, compound):
        # type: (Compound) -> None
        with self._mutex:
            self.compound_fragment_map.pop(compound)

    def removeCompoundByName(self, name):
        # type: (CompoundName) -> Optional[Compound]
        with self._mutex:
            for c in self.compound_fragment_sequence_map.keys():
                if c.compound_name == name:
                    self.compound_fragment_sequence_map.pop(c)
                    if c in self.compound_fragment_map:
                        self.compound_fragment_map.pop(c)
                    return c
            return None

    def getPendingCompounds(self):
        # type: () -> List[Compound]
        with self._mutex:
            return list(self.compound_fragment_map.keys())

    def getPendingCompoundWithName(self, name):
        # type: (CompoundName) -> Optional[Compound]
        with self._mutex:
            for c in self.compound_fragment_sequence_map.keys():
                if c.compound_name == name:
                    return c
            return None

    def getFragmentsNeededForPendingCompound(self, name):
        # type: (CompoundName) -> Optional[List[Tuple[Fragment, SequenceIndex]]]
        with self._mutex:
            for c in self.compound_fragment_sequence_map.keys():
                if c.compound_name == name:
                    return list(self.compound_fragment_sequence_map[c])
            return None


    def getPendingFragments(self):
        # type: () -> List[Fragment]
        with self._mutex:
            return list(self.pending_fragments.values())

    def popNonPendingFragmentSequences(self):
        with self._mutex:
            return_dict = {}  # type: Dict[Compound, List[Tuple[Fragment, SequenceIndex]]]
            for c in list(self.compound_fragment_sequence_map.keys()):
                if c not in self.compound_fragment_map:
                    # print(c.compound_name, 'not in compound_fragment_map')
                    return_dict[c] = self.compound_fragment_sequence_map.pop(c)
            return return_dict
