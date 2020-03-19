from typing import Type, Union

from .BaseWrapper import BaseWrapper


class StackedWrapper(BaseWrapper):
    def __init__(self, *wrapper_sequence):
        # type: (*Union[BaseWrapper, Type[BaseWrapper]]) -> None
        for w in wrapper_sequence:
            if isinstance(w, self.__class__) or issubclass(type(w), self.__class__):
                raise RuntimeError("cannot use a stacked wrapper inside of another stacked wrapper")
        _wrappers = list(wrapper_sequence)
        self._wrappers = tuple(_wrappers)
        _wrappers.reverse()
        self._unwrappers = tuple(_wrappers)

    def get_wrapper_type(self, instance=None):
        return '-'.join((w.get_wrapper_type() for w in self._wrappers))

    def wrap(self, data):
        for wrapper in self._wrappers:
            data = wrapper.wrap(data)
        return data

    def unwrap(self, data):
        for unwrapper in self._unwrappers:
            data = unwrapper.unwrap(data)
        return data
