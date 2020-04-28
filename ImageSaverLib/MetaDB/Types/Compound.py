from typing import NewType

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
# CompoundPendingFlag = NewType('CompoundPendingFlag', bool)


class Compound(Base, ColumnPrinterMixin):
    FILE_TYPE = 'File'
    DIR_TYPE = 'Dir'

    __tablename__ = 'compounds'
    compound_id = Column(Integer, Sequence('compound_id_seq'), primary_key=True, unique=True, index=True)  # type: CompoundID
    compound_name = Column(Text, unique=True, index=True)  # type: CompoundName
    compound_type = Column(String(255))  # type: CompoundType
    compound_hash = Column(LargeBinary(64))  # type: CompoundHash
    compound_size = Column(BigInteger)  # type: CompoundSize
    wrapping_type = Column(String(255))  # type: CompoundWrappingType
    compression_type = Column(String(255))  # type: CompoundCompressionType
    # compound_pending = Column(Boolean)  # type: CompoundPendingFlag
    __table_args__ = (UniqueConstraint('compound_name', 'compound_hash', 'compound_type'),
                      # UniqueConstraint('compound_name', 'compound_pending')
                      )

    def __init__(self, compound_name, compound_type, compound_hash, compound_size, wrapping_type, compression_type):
        # type: (CompoundName, CompoundType, CompoundHash, CompoundSize, CompoundWrappingType, CompoundCompressionType) -> None
        self.compound_name = compound_name
        self.compound_type = compound_type
        self.compound_hash = compound_hash
        self.compound_size = compound_size
        self.wrapping_type = wrapping_type
        self.compression_type = compression_type
        # self.payload_id = payload_id
