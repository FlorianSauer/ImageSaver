from typing import NewType

from . import ColumnPrinterMixin
from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint, Sequence

from .. import Base
from .Compound import CompoundID
from .Fragment import FragmentID

CompoundFragmentMappingID = NewType('CompoundFragmentMappingID', int)
SequenceIndex = NewType('SequenceIndex', int)


class CompoundFragmentMapping(Base, ColumnPrinterMixin):
    __tablename__ = 'compound_fragment_mapping'
    payload_fragment_id = Column(Integer, Sequence('compound_fragment_mapping_id_seq'), primary_key=True, unique=True,
                                 nullable=False)  # type: CompoundFragmentMappingID
    compound_id = Column(Integer, ForeignKey('compounds.compound_id', ondelete='CASCADE'),
                         nullable=False, index=True)  # type: CompoundID
    fragment_id = Column(Integer, ForeignKey('fragments.fragment_id', ondelete='CASCADE'),
                         nullable=False, index=True)  # type: FragmentID
    sequence_index = Column(Integer, nullable=False)  # type: SequenceIndex
    __table_args__ = (UniqueConstraint('compound_id', 'sequence_index'),)

    def __init__(self, compound_id, fragment_id, sequence_index):
        # type: (CompoundID, FragmentID, SequenceIndex) -> None
        self.compound_id = compound_id
        self.fragment_id = fragment_id
        self.sequence_index = sequence_index
