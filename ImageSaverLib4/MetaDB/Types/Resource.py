import hashlib
from typing import NewType

from ImageSaverLib4.Encapsulation import CompressionType, WrappingType
from ImageSaverLib4.MetaDB.Types import ColumnPrinterMixin
from sqlalchemy import Column, Integer, String, LargeBinary, UniqueConstraint, BigInteger, Sequence

from ImageSaverLib4.MetaDB import Base

ResourceID = NewType('ResourceID', int)
ResourceName = NewType('ResourceName', str)
ResourceSize = NewType('ResourceSize', int)
ResourcePayloadSize = NewType('ResourcePayloadSize', int)
ResourceHash = NewType('ResourceHash', bytes)
ResourceCompressionType = NewType('ResourceCompressionType', CompressionType)
ResourceWrappingType = NewType('ResourceWrappingType', WrappingType)


class Resource(Base, ColumnPrinterMixin):
    __tablename__ = 'resources'
    resource_id = Column(Integer, Sequence('resource_id_seq'), primary_key=True, unique=True)  # type: ResourceID
    resource_name = Column(String(255), unique=True, nullable=False)  # type: ResourceName
    resource_size = Column(BigInteger)  # type: ResourceSize
    resource_payloadsize = Column(BigInteger)  # type: ResourcePayloadSize
    resource_hash = Column(LargeBinary(64), unique=True)  # type: ResourceHash
    wrapping_type = Column(String(255))  # type: ResourceWrappingType
    compression_type = Column(String(255))  # type: ResourceCompressionType
    __table_args__ = (UniqueConstraint('resource_name', 'resource_hash'), )

    def __init__(self, resource_name, resource_size, resource_payloadsize, resource_hash, wrapping_type, compression_type):
        # type: (ResourceName, ResourceSize, ResourcePayloadSize, ResourceHash, ResourceWrappingType, ResourceCompressionType) -> None
        self.resource_name = resource_name  # name of uploaded file/image/...
        self.resource_size = resource_size  # the size of the uploaded resource
        self.resource_payloadsize = resource_payloadsize  # the size of the uploaded resource, without encapsulation
        self.resource_hash = resource_hash  # the hash of the uploaded resource, for checking correct download
        self.wrapping_type = wrapping_type  # used for extraction of block payload from resource
        self.compression_type = compression_type  # used for extraction of block payload from resource

    @classmethod
    def makeResourceHash(cls, resource_data):
        # type: (bytes) -> ResourceHash
        return ResourceHash(hashlib.sha256(resource_data).digest())
