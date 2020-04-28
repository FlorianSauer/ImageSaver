from Crypto.Cipher import AES
# noinspection PyProtectedMember
from Crypto.Util import Counter

from ..BaseWrapper import BaseWrapper

TEST_KEY = 'f7ab933b57af750f60a69fac250c27dee3cfd16513491446c2ddcaf8e792fc29'


class AES256CTRWrapper(BaseWrapper):
    _wrapper_type = 'aes256'

    def __init__(self, key=bytes.fromhex(TEST_KEY)):
        # type: (bytes) -> None
        if len(key) != 32:
            raise ValueError("key size must be 32 bytes")
        self._key = key

    def encrypt_once(self, s):
        # type: (bytes) -> bytes
        aes = AES.new(self._key, AES.MODE_CTR, counter=Counter.new(128))
        return aes.encrypt(s)

    def decrypt_once(self, s):
        # type: (bytes) -> bytes
        aes = AES.new(self._key, AES.MODE_CTR, counter=Counter.new(128))
        return aes.decrypt(s)

    def wrap(self, data):
        # print(self.__class__.__name__, "wrapping", len(data), "bytes")
        return self.encrypt_once(data)

    def unwrap(self, data):
        # print(self.__class__.__name__, "unwrapping", len(data), "bytes")
        return self.decrypt_once(data)
