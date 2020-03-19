import hashlib
import struct

from ..BaseWrapper import BaseWrapper
from ..WrapperErrors import UnWrapError


class SizeChecksumWrapper(BaseWrapper):
    _wrapper_type = 'sc'

    _int_struct = struct.Struct('!I')
    _int_struct_len = 4
    _hash_len = len(hashlib.sha256(b'').digest())

    @classmethod
    def wrap(cls, chunk):
        return cls._int_struct.pack(len(chunk)) + chunk + hashlib.sha256(chunk).digest()

    @classmethod
    def unwrap(cls, chunk):
        if len(chunk) < cls._int_struct_len + cls._hash_len:
            raise UnWrapError("Chunk is too small")
        chunk_len = cls._int_struct.unpack(chunk[:cls._int_struct_len])[0]
        chunk_hash = chunk[-cls._hash_len:]
        chunk_data = chunk[cls._int_struct_len:-cls._hash_len]

        if len(chunk_data) != chunk_len:
            raise UnWrapError("Chunk data is unequal to expected length")
        if chunk_hash != hashlib.sha256(chunk_data).digest():
            raise UnWrapError("Chunk data is unequal to expected hash")
        return chunk_data
