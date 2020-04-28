import lzma

from ..BaseCompressor import BaseCompressor


class LZMACompressor(BaseCompressor):

    _compresser_type = 'lzma'
    @classmethod
    def compress(cls, data):
        return lzma.compress(data)

    @classmethod
    def decompress(cls, data):
        return lzma.decompress(data)
