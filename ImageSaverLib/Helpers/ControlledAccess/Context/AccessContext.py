from abc import ABC, abstractmethod
from typing import Generic, Optional

from .. import V
from ..AccessManager import AccessManager


class AccessContext(ABC, Generic[V]):

    def __init__(self, accessmanager, value, blocking=True, timeout=None):
        # type: (AccessManager[V], V, bool, Optional[float]) -> None
        self._accessmanager = accessmanager
        self._value = value
        self._blocking = blocking
        self._timeout = timeout

    def __enter__(self):
        self.enter_func(self._accessmanager, self._value, self._blocking, self._timeout)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit_func(self._accessmanager, self._value)
        if exc_type:
            raise

    @staticmethod
    @abstractmethod
    def enter_func(accessmanager, value, blocking=True, timeout=None):
        # type: (AccessManager[V], V, bool, Optional[float]) -> None
        pass

    @staticmethod
    @abstractmethod
    def exit_func(accessmanager, value):
        # type: (AccessManager[V], V) -> None
        pass
