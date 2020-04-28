import hashlib
from itertools import zip_longest
from typing import BinaryIO, Generator, Iterable, Optional, Iterator, TypeVar, Tuple

import math


def get_sha256_of_bytes(data):
    # type: (bytes) -> bytes
    return hashlib.sha256(data).digest()


def get_sha256_of_stream(stream, chunksize=65536):
    # type: (BinaryIO, int) -> bytes
    cursor_index = stream.tell()
    stream.seek(0)
    sha = hashlib.sha256()
    while True:
        data = stream.read(chunksize)
        if not data:
            break
        sha.update(data)
    stream.seek(cursor_index)
    return sha.digest()


def get_size_of_stream(stream):
    # type: (BinaryIO) -> int
    c = stream.tell()
    stream.seek(0, 2)
    s = stream.tell()
    stream.seek(c)
    return s


def calculate_chunkcount(stream, chunksize):
    # type: (BinaryIO, int) -> int
    filesize = get_size_of_stream(stream)
    return int(math.ceil(filesize / chunksize))


def chunkfile(stream, chunksize):
    # type: (BinaryIO, int) -> Generator[bytes, None, None]
    c = stream.tell()
    stream.seek(0)
    while True:
        data = stream.read(chunksize)
        if not data:
            break
        yield data
    stream.seek(c)


T = TypeVar('T')


def chunkiterable(iterable, n, fillvalue=None):
    # type: (Iterable[T], int, Optional[T]) -> Iterator[Tuple[T]]
    """
    Collect data into fixed-length chunks or blocks

    chunkiterable('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    """
    raise DeprecationWarning('Use chunkiterable_gen instead')
    args = [iter(iterable)] * n
    # args = (iter(iterable) for _ in range(n))
    return zip_longest(*args, fillvalue=fillvalue)


def chunkiterable_gen(iterable, n, fillvalue=None, skip_none=False):
    # type: (Iterable[T], int, Optional[T], bool) -> Iterator[Tuple[T]]
    buffer = []
    for index, item in enumerate(iterable):
        buffer.append(item)
        if (index + 1) % n == 0:
            if len(buffer) < n and fillvalue:
                buffer += [fillvalue] * (n - len(buffer))
            yield tuple(buffer)
            buffer.clear()
    if fillvalue is None and skip_none:
        yield tuple(buffer)
    else:
        if len(buffer) < n:
            buffer += [fillvalue] * (n - len(buffer))
        yield tuple(buffer)


def split_bytes(b, index):
    # type: (bytes, int) -> Tuple[bytes, bytes]
    return b[:index], b[index:]
