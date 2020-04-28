import zlib

from ..BaseCompressor import BaseCompressor


class ZLibCompressor(BaseCompressor):
    _compresser_type = 'zlib'
    @classmethod
    def compress(cls, data):
        return zlib.compress(data)

    @classmethod
    def decompress(cls, data):
        return zlib.decompress(data)
