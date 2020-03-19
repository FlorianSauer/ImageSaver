from typing import Optional, Any, Type

from .BaseWrapper import BaseWrapper


class BaseWrapperFactory(BaseWrapper):
    """
    Abstract class, which creates a new wrapper context. Useful to keep the wrapping state across multiple wrapping
    operations.

    Example: AES Wrapper - no new wrapper per fragment is needed. Instead one wrapper per tesource/compound is
    recommended
    """

    def __init__(self, bound_wrapper, *args, **kwargs):
        # type: (Type[BaseWrapper], *Any, **Any) -> None
        self.current_wrapper = None  # type: Optional[BaseWrapper]
        self.bound_wrapper = bound_wrapper
        self.args = args
        self.kwargs = kwargs

    def buildWrapper(self):
        # type: () -> BaseWrapper
        # noinspection PyArgumentList
        return self.bound_wrapper(*self.args, **self.kwargs)

    def resetContext(self):
        self.current_wrapper = None  # type: Optional[BaseWrapper]

    def wrap(self, data):
        if not self.current_wrapper:
            self.current_wrapper = self.buildWrapper()
        return self.current_wrapper.wrap(data)

    def unwrap(self, data):
        if not self.current_wrapper:
            self.current_wrapper = self.buildWrapper()
        return self.current_wrapper.unwrap(data)

    def get_wrapper_type(cls, instance=None):
        return self.bound_wrapper.get_wrapper_type()

    def set_wrapper_type(self, value):
        self.bound_wrapper.set_wrapper_type(value)

