from abc import ABC, abstractmethod
from . import CompressionType


class BaseCompressor(ABC):
    """
    Basic Class for de-/compressing a payload.
    """

    _compresser_type = None

    @classmethod
    def get_compressor_type(cls):
        # type: () -> CompressionType
        if cls._compresser_type is None:
            cls._compresser_type = CompressionType(cls.__name__)
        return cls._compresser_type

    @classmethod
    def set_compresser_type(cls, value):
        cls._compresser_type = value

    @classmethod
    @abstractmethod
    def compress(cls, data):
        # type: (bytes) -> bytes
        pass

    @classmethod
    @abstractmethod
    def decompress(cls, data):
        # type: (bytes) -> bytes
        pass

