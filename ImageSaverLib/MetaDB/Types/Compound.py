from typing import NewType, Optional

from ImageSaverLib.Encapsulation import WrappingType, CompressionType
from ImageSaverLib.MetaDB.Types import ColumnPrinterMixin
from sqlalchemy import Column, Integer, String, UniqueConstraint, LargeBinary, BigInteger, Sequence, Text

from ImageSaverLib.MetaDB import Base

CompoundID = NewType('CompoundID', int)
CompoundName = NewType('CompoundName', str)
CompoundType = NewType('CompoundType', str)
CompoundHash = NewType('CompoundHash', bytes)
CompoundSize = NewType('CompoundSize', int)
CompoundCompressionType = NewType('CompoundCompressionType', CompressionType)
CompoundWrappingType = NewType('CompoundWrappingType', WrappingType)
CompoundVersion = NewType('CompoundVersion', Optional[int])
"""CompoundVersion == None -> Live version,
   CompoundVersion >= 0 -> The N-th version of a compound"""

# CompoundPendingFlag = NewType('CompoundPendingFlag', bool)


class Compound(Base, ColumnPrinterMixin):
    FILE_TYPE = 'File'
    DIR_TYPE = 'Dir'

    __tablename__ = 'compounds'
    compound_id = Column(Integer, Sequence('compound_id_seq'), primary_key=True, unique=True, index=True)  # type: CompoundID
    compound_name = Column(Text, unique=False, index=True)  # type: CompoundName
    compound_type = Column(String(255))  # type: CompoundType
    compound_hash = Column(LargeBinary(64))  # type: CompoundHash
    compound_size = Column(BigInteger)  # type: CompoundSize
    wrapping_type = Column(String(255))  # type: CompoundWrappingType
    compression_type = Column(String(255))  # type: CompoundCompressionType
    compound_version = Column(Integer)  # type: CompoundVersion

    # compound_pending = Column(Boolean)  # type: CompoundPendingFlag
    __table_args__ = (UniqueConstraint('compound_name', 'compound_hash', 'compound_type', 'compound_version'),
                      UniqueConstraint('compound_name', 'compound_version')
                      )

    def __init__(self, compound_name, compound_type, compound_hash, compound_size, wrapping_type, compression_type, compound_version=None):
        # type: (CompoundName, CompoundType, CompoundHash, CompoundSize, CompoundWrappingType, CompoundCompressionType, CompoundVersion) -> None
        self.compound_name = compound_name
        self.compound_type = compound_type
        self.compound_hash = compound_hash
        self.compound_size = compound_size
        self.wrapping_type = wrapping_type
        self.compression_type = compression_type
        self.compound_version = compound_version
        # self.payload_id = payload_id
