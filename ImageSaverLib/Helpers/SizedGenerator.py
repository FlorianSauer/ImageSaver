from types import TracebackType
from typing import Generator, Generic, TypeVar, Type, Optional, Sized, cast, Callable, Union

Y = TypeVar('Y')
S = TypeVar('S')
R = TypeVar('R')
_Y = TypeVar('_Y')
_S = TypeVar('_S')
_R = TypeVar('_R')


class SizedGenerator(Generator, Sized, Generic[Y, S, R, _Y, _S, _R]):
    """
    A Generator wrapper with a length attribute.
    Useful for memory efficient generators with a known size, like queries from a huge table with a known size.
    """
    def __init__(self, gen, length=None):
        # type: (Generator[Y, S, R], Union[Optional[int], Callable[[], int]]) -> None
        # print(gen, length)
        if length is None:
            assert type(gen) is SizedGenerator
        self.gen = gen
        if type(length) is int:
            self._length_callback = None
            self._length = length
        elif callable(length):
            self._length_callback = length
            self._length = None
        else:
            assert type(gen) is SizedGenerator
            assert length is None
            self._length_callback = cast(SizedGenerator, gen)._length_callback
            self._length = cast(SizedGenerator, gen)._length
        assert self._length_callback is not None or self._length is not None

    def __iter__(self):
        # type: () -> Generator[Y, S, R]
        return self.gen.__iter__()

    def __next__(self):
        # type: () -> Y
        return self.gen.__next__()

    def send(self, value):
        # type: (S) -> Y
        return self.gen.send(value)

    def throw(self, typ, val=None, tb=None):
        # type: (Type[BaseException], Optional[BaseException], Optional[TracebackType]) -> Y
        return self.gen.throw(typ, val, tb)

    def close(self):
        # type: () -> None
        return self.gen.close()

    def __len__(self):
        assert self._length_callback is not None or self._length is not None
        if self._length is not None:
            # print("length by int")
            return self._length
        elif self._length_callback is not None:
            # print("length by callback")
            len = self._length_callback()
            self._length = len
            assert type(len) is int
            return len
        else:
            raise RuntimeError("No length given during init")

    @classmethod
    def layer_adder(cls, len_gen, layer=lambda x: (i for i in x)):
        # type: (SizedGenerator[Y, S, R], Callable[[Generator[Y, S, R]], Generator[_Y, _S, _R]]) -> SizedGenerator[_Y, _S, _R]
        return cls(layer(len_gen), len(len_gen))

    def add_layer(self, layer=lambda gen: (i for i in gen)):
        # type: (Callable[[Generator[Y, S, R]], Generator[_Y, _S, _R]]) -> SizedGenerator[_Y, _S, _R]
        if self._length_callback:
            return self.__class__(layer(self), self._length_callback)
        else:
            return self.__class__(layer(self), self._length)
