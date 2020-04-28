from typing import NewType

from .. import Base
from . import ColumnPrinterMixin
from .Fragment import FragmentID
from .Resource import ResourceID
from sqlalchemy import Column, Integer, ForeignKey, BigInteger, UniqueConstraint, Sequence

FragmentResourceMappingID = NewType('FragmentResourceMappingID', int)
FragmentOffset = NewType('FragmentOffset', int)


class FragmentResourceMapping(Base, ColumnPrinterMixin):
    __tablename__ = 'fragment_resource_mappings'
    fragment_resource_mapping_id = Column(Integer, Sequence('fragment_resource_mapping_id_seq'), primary_key=True, unique=True)  # type: FragmentResourceMappingID

    fragment_id = Column(Integer, ForeignKey('fragments.fragment_id', ondelete='CASCADE'),
                         nullable=False, unique=True, index=True)  # type: FragmentID
    resource_id = Column(Integer, ForeignKey('resources.resource_id', ondelete='CASCADE'),
                         nullable=False, unique=False, index=True)  # type: ResourceID
    fragment_offset = Column(BigInteger)  # type: FragmentOffset

    __table_args__ = (UniqueConstraint('fragment_id', 'resource_id'), )

    def __init__(self, fragment_id, resource_id, fragment_offset):
        # type: (FragmentID, ResourceID, FragmentOffset) -> None
        self.fragment_id = fragment_id
        self.resource_id = resource_id
        self.fragment_offset = fragment_offset
