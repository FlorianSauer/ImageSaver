from types import TracebackType
from typing import BinaryIO, Optional, Type, Iterator, AnyStr, Iterable, List


class DevNull(BinaryIO):

    def write(self, s: AnyStr) -> int:
        return 0

    def close(self) -> None:
        pass

    def fileno(self) -> int:
        return 0

    def flush(self) -> None:
        pass

    def isatty(self) -> bool:
        return False

    def read(self, n: int = ...) -> AnyStr:
        return b''

    def readable(self) -> bool:
        return False

    def readline(self, limit: int = ...) -> AnyStr:
        return b''

    def readlines(self, hint: int = ...) -> List[AnyStr]:
        return [b'']

    def seek(self, offset: int, whence: int = ...) -> int:
        return 0

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return 0

    def truncate(self, size: Optional[int] = ...) -> int:
        return 0

    def writable(self) -> bool:
        return True

    def writelines(self, lines: Iterable[AnyStr]) -> None:
        pass

    def __next__(self) -> AnyStr:
        return b''

    def __iter__(self) -> Iterator[AnyStr]:
        return b''

    def __enter__(self) -> BinaryIO:
        return self

    def __exit__(self, t, value, traceback):
        # type: (Optional[Type[BaseException]], Optional[BaseException], Optional[TracebackType]) -> bool
        # type comments needed to fix the bug #266 on python 3.5.2
        return True
