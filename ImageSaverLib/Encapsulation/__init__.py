from typing import Union, Type

__all__ = ['encapsulate', 'decapsulate', 'makeWrappingType', 'makeCompressingType', 'BaseWrapper', 'BaseCompressor',
           'WrappingType', 'CompressionType', 'AutoWrapper', 'AutoCompressor']

from .Compressors.AutoCompressor import AutoCompressor
from .Compressors import CompressionType
from .Wrappers.AutoWrapper import AutoWrapper
from .Wrappers import WrappingType
from .Compressors.BaseCompressor import BaseCompressor
from .Wrappers.BaseWrapper import BaseWrapper


def encapsulate(auto_compresser, auto_wrapper, compress_type, wrap_type, data):
    # type: (AutoCompressor, AutoWrapper, CompressionType, WrappingType, bytes) -> bytes
    """
    Processes given plain data with compressers and wrappers to encapsulated data
    """
    return auto_wrapper.wrap(auto_compresser.compress(data, compress_type), wrap_type)


def decapsulate(auto_compresser, auto_wrapper, compress_type, wrap_type, data):
    # type: (AutoCompressor, AutoWrapper, CompressionType, WrappingType, bytes) -> bytes
    """
    Processes given encapsulated data with compressers and wrappers to plain data
    """
    return auto_compresser.decompress(auto_wrapper.unwrap(data, wrap_type), compress_type)


def makeWrappingType(*wrappers):
    # type: (*Union[Union[Type[BaseWrapper], BaseWrapper]]) -> WrappingType
    if len(wrappers) > 0:
        return WrappingType('-'.join((t.get_wrapper_type() for t in wrappers)))
    else:
        return wrappers[0].get_wrapper_type()


def makeCompressingType(*compressers):
    # type: (*Union[Union[Type[BaseCompressor], BaseCompressor]]) -> CompressionType
    if len(compressers) > 0:
        return CompressionType('-'.join((t.get_compressor_type() for t in compressers)))
    else:
        return compressers[0].get_compressor_type()
