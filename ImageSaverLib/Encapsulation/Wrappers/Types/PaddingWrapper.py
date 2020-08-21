from cryptography.hazmat.primitives import padding

from ..BaseWrapper import BaseWrapper
from ..WrapperErrors import UnWrapError


class PaddingWrapper(BaseWrapper):
    _wrapper_type = 'pkcs7pad'

    def __init__(self, block_len=128):
        if not block_len % 8 or not 0 < block_len < 2040:
            raise ValueError("block size is not a multiple of 8 or is not between 0 and 2040")
        self.block_len = block_len
        self.padder = padding.PKCS7(128)

    def wrap(self, data):
        padder = self.padder.padder()
        padded_data = padder.update(data)
        padded_data += padder.finalize()
        return padded_data

    def unwrap(self, data):
        padder = self.padder.unpadder()
        unpadded_data = padder.update(data)
        try:
            unpadded_data += padder.finalize()
        except ValueError:
            raise UnWrapError("given data is too small")
        return unpadded_data
