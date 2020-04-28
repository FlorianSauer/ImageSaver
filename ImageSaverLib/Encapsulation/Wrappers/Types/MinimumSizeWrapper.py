import os
import struct

from ..BaseWrapper import BaseWrapper
from ..WrapperErrors import UnWrapError


class MinimumSizeWrapper(BaseWrapper):
    _wrapper_type = 'ms'

    _int_struct = struct.Struct('!I')
    _int_struct_len = 4

    def __init__(self, minimum_size=0):
        # type: (int) -> None
        self.minimum_size = minimum_size
        self._wrapper_type += str(minimum_size)
        # print(self._wrapper_type, self.get_wrapper_type())

    def get_wrapper_type(self, instance=None):
        return super().get_wrapper_type(self)

    def wrap(self, chunk):
        if self.minimum_size < self._int_struct_len:
            return chunk
        fill_size = self.minimum_size - len(chunk)
        fill_size -= self._int_struct_len
        if fill_size < 0:
            fill_size = 0
        return self._int_struct.pack(len(chunk)) + chunk + os.urandom(fill_size)

    def unwrap(self, chunk):
        if self.minimum_size < self._int_struct_len:
            return chunk
        if len(chunk) < self.minimum_size:
            raise UnWrapError("Chunk is too small")
        if len(chunk) < self._int_struct_len:
            return chunk
        chunk_len = self._int_struct.unpack(chunk[:self._int_struct_len])[0]
        if len(chunk) - self._int_struct_len < chunk_len:
            raise UnWrapError("Chunk data is smaller than expected length")
        chunk_data = chunk[self._int_struct_len:chunk_len+self._int_struct_len]
        if len(chunk_data) != chunk_len:
            raise UnWrapError("Chunk data is unequal to expected length")
        return chunk_data
