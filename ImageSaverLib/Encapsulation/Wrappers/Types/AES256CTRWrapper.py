import hashlib

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import algorithms, modes, Cipher, CipherContext

from ..BaseWrapper import BaseWrapper


class AES256CTRWrapper(BaseWrapper):
    _wrapper_type = 'aes256'

    def __init__(self, key):
        # type: (bytes) -> None
        if len(key) != 32:
            raise ValueError("key size must be 32 bytes")
        self._key = key
        self._nonce = hashlib.md5(self._key).digest()

    def _getCipher(self):
        # type: () -> Cipher
        algorithm = algorithms.AES(self._key)
        mode = modes.CTR(self._nonce)
        return Cipher(algorithm, mode, default_backend())

    def _getEncryptor(self):
        # type: () -> CipherContext
        return self._getCipher().encryptor()

    def _getDecryptor(self):
        # type: () -> CipherContext
        return self._getCipher().decryptor()

    def encrypt_once(self, s):
        # type: (bytes) -> bytes
        encryptor = self._getEncryptor()
        return encryptor.update(s) + encryptor.finalize()

    def decrypt_once(self, s):
        # type: (bytes) -> bytes
        decryptor = self._getDecryptor()
        return decryptor.update(s) + decryptor.finalize()

    def wrap(self, data):
        return self.encrypt_once(data)

    def unwrap(self, data):
        return self.decrypt_once(data)
