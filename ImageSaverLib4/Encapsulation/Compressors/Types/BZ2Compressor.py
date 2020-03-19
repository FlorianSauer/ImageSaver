import bz2

from ..BaseCompressor import BaseCompressor


class BZ2Compressor(BaseCompressor):
    _compresser_type = 'bz2'

    @classmethod
    def compress(cls, data):
        return bz2.compress(data)

    @classmethod
    def decompress(cls, data):
        return bz2.decompress(data)
