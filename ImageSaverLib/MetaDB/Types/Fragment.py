from typing import NewType

from sqlalchemy import Column, Integer, LargeBinary, BigInteger, Sequence

from ImageSaverLib.MetaDB import Base
from ImageSaverLib.MetaDB.Types import ColumnPrinterMixin

FragmentID = NewType('FragmentID', int)
FragmentHash = NewType('FragmentHash', bytes)
FragmentSize = NewType('FragmentSize', int)
FragmentPayloadSize = NewType('FragmentPayloadSize', int)


class Fragment(Base, ColumnPrinterMixin):
    __tablename__ = 'fragments'
    fragment_id = Column(Integer, Sequence('fragment_id_seq'), primary_key=True, unique=True, index=True)  # type: FragmentID
    fragment_hash = Column(LargeBinary(64), unique=True, index=True)  # type: FragmentHash
    fragment_size = Column(BigInteger)  # type: FragmentSize
    fragment_payload_size = Column(BigInteger)  # type: FragmentPayloadSize

    def __init__(self, fragment_hash, fragment_size, fragment_payload_size):
        # type: (FragmentHash, FragmentSize, FragmentPayloadSize) -> None
        self.fragment_hash = fragment_hash
        self.fragment_size = fragment_size
        self.fragment_payload_size = fragment_payload_size
        # self.fragment_pending = pending

    def __hash__(self):
        return self.fragment_hash.__hash__()
