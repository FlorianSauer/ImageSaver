from typing import Dict, Type, Union, List

from .BaseCompressor import BaseCompressor
from .StackedCompressor import StackedCompressor
from .Types import (BZ2Compressor, LZMACompressor, PassThroughCompressor, ZLibCompressor)
from . import CompressionType


class AutoCompressor(object):
    def __init__(self):
        self.compressor_mappings = {}  # type: Dict[str, Type[BaseCompressor]]
        self.addCompressor(BZ2Compressor())
        self.addCompressor(LZMACompressor())
        self.addCompressor(PassThroughCompressor())
        self.addCompressor(ZLibCompressor())

    def addCompressor(self, compressor):
        # type: (Union[Type[BaseCompressor], BaseCompressor]) -> None
        self.compressor_mappings[compressor.get_compressor_type()] = compressor

    def getStackedCompressor(self, compress_type):
        # type: (Union[CompressionType, List[CompressionType]]) -> Union[StackedCompressor, Type[BaseCompressor], BaseCompressor]
        if type(compress_type) is list:
            compress_types = [ct for ct in compress_type]
        else:
            compress_type = compress_type.lower()
            compress_types = compress_type.split('-')
        compressors = []
        for ct in compress_types:
            if ct not in self.compressor_mappings:
                raise UnsupportedCompressorType("not supported compressor " + repr(ct))
            compressors.append(self.compressor_mappings[ct])
        if len(compressors) == 1:
            return compressors[0]
        else:
            assert len(compressors) > 0
            return StackedCompressor(*compressors)

    def compress(self, data, compress_type):
        # type: (bytes, CompressionType) -> bytes
        compressor = self.getStackedCompressor(compress_type)
        return compressor.compress(data)

    def decompress(self, data, compress_type):
        # type: (bytes, CompressionType) -> bytes
        decompressor = self.getStackedCompressor(compress_type)
        return decompressor.decompress(data)


class UnsupportedCompressorType(Exception):
    pass
