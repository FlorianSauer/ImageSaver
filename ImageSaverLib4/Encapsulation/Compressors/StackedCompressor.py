from .BaseCompressor import BaseCompressor


class StackedCompressor(BaseCompressor):
    def __init__(self, *compressor_sequence):
        # type: (*BaseCompressor) -> None
        for w in compressor_sequence:
            if isinstance(w, self.__class__) or issubclass(type(w), self.__class__):
                raise RuntimeError("cannot use a stacked compressor inside of another stacked wrapper")
        _compressors = list(compressor_sequence)
        self._compressors = tuple(_compressors)
        _compressors.reverse()
        self._decompressors = tuple(_compressors)

    def get_compressor_type(self):
        return '-'.join((w.get_compressor_type() for w in self._compressors))

    def compress(self, data):
        for compresser in self._compressors:
            data = compresser.compress(data)
        return data

    def decompress(self, data):
        for decompresser in self._decompressors:
            data = decompresser.decompress(data)
        return data
