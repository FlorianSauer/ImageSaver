from abc import ABC, abstractmethod

from . import WrappingType


class BaseWrapper(ABC):
    """
    Basic Class for wrapping/hiding a payload inside of other data.
    """

    _wrapper_type = None

    @classmethod
    def get_wrapper_type(cls, instance=None):
        # type: (BaseWrapper) -> WrappingType
        if instance:
            wrapper = instance
        else:
            wrapper = cls
        if wrapper._wrapper_type is None:
            wrapper._wrapper_type = WrappingType(cls.__name__)
        if not wrapper._wrapper_type:
            raise RuntimeError("Wrapper Type is empty")
        return wrapper._wrapper_type

    @classmethod
    def set_wrapper_type(cls, value):
        cls._wrapper_type = value

    @classmethod
    @abstractmethod
    def wrap(cls, data):
        # type: (bytes) -> bytes
        pass

    @classmethod
    @abstractmethod
    def unwrap(cls, data):
        # type: (bytes) -> bytes
        pass
