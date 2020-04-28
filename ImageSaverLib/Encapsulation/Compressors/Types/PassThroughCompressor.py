from ..BaseCompressor import BaseCompressor


class PassThroughCompressor(BaseCompressor):

    _compresser_type = 'pass'
    @classmethod
    def compress(cls, data):
        return data

    @classmethod
    def decompress(cls, data):
        return data
