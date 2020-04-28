from ..BaseWrapper import BaseWrapper
from ..WrapperErrors import UnWrapError
from Crypto.Util.Padding import pad, unpad
import struct


class PaddingWrapper(BaseWrapper):
    _wrapper_type = 'pad'

    PKCS7 = 1
    ISO7816 = 2
    X923 = 3

    PADDING_NAMES = {
        PKCS7: 'pkcs7',
        ISO7816: 'iso7816',
        X923: 'x923'
    }

    _byte_struct = struct.Struct('!B')
    _byte_struct_len = 1

    _int_struct = struct.Struct('!H')
    _int_struct_len = 2

    def __init__(self, block_len=16, algo=PKCS7):
        if not 16 <= block_len <= 65535:
            raise ValueError("requires 0 <= block_len <= 65535")
        self.default_block_len = block_len
        self.default_algo = algo
        self.default_algo_name = self.PADDING_NAMES[self.default_algo]
        self.default_padding_bytes = self._byte_struct.pack(self.default_algo)
        self.default_block_len_bytes = self._int_struct.pack(self.default_block_len)

    def wrap(self, data):
        return self.default_padding_bytes+self.default_block_len_bytes+pad(data, self.default_block_len, self.default_algo_name)

    def unwrap(self, data):
        if len(data) > self._byte_struct_len+self._int_struct_len:
            algo = self._byte_struct.unpack(data[0:self._byte_struct_len])[0]
            block_len = self._int_struct.unpack(data[self._byte_struct_len:self._byte_struct_len+self._int_struct_len])[0]
            return unpad(data[self._byte_struct_len+self._int_struct_len:], block_len, self.PADDING_NAMES[algo])
        raise UnWrapError("given data is too small")
