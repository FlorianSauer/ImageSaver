import io
from typing import Generator, Optional


class FileLikeIterator(io.RawIOBase):
    def __init__(self, iterator):
        # type: (Generator[bytes]) -> None
        self.iterator = iterator
        self.leftover = None
        self.seek_index = 0

    def readinto(self, b):
        try:
            l = len(b)  # We're supposed to return at most this much
            # print("We're supposed to return at most this much", l)
            chunk = self.leftover or next(self.iterator)
            output, self.leftover = chunk[:l], chunk[l:]
            b[:len(output)] = output
            return len(output)
        except StopIteration:
            return 0  # indicate EOF

    def read(self, size=None):
        # type: (Optional[int]) -> Optional[bytes]
        chunk = super().read(size)
        if chunk:
            self.seek_index += len(chunk)
        return chunk

    def readable(self):
        return True

    def seekable(self):
        # type: () -> bool
        return False

    def tell(self):
        # type: () -> int
        return self.seek_index
