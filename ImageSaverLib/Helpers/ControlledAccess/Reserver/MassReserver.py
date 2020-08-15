from abc import abstractmethod
from typing import Generic, Optional, Iterator, Union, List

from .. import V
from ..AccessManager import AccessManager


class MassReserver(Generic[V]):
    """
    Context object to reserve all given values at once.
    If the reserving for one value fails, all already reserved values will be unreserved.

    After all values are reserved, one or all values can be unreserved.

    The MassReserver should not be shared across threads.
    The MassReserver itself is NOT thread-save
    """

    def __init__(self, accessmanager, *values, values_gen=None, blocking=True, timeout=None):
        # type: (AccessManager[V], *V, Optional[Iterator[V]], bool, Optional[float]) -> None
        self._accessmanager = accessmanager
        if values_gen:
            self._unreserved_values = list(values_gen)
        else:
            self._unreserved_values = list(values)
        self._reserved_values = []
        self._blocking = blocking
        self._timeout = timeout
        self._with_started = False

    @staticmethod
    @abstractmethod
    def _reserveAll(accessmanager, *values, blocking, timeout=None):
        # type: (AccessManager[V], Union[*V, Iterator[V]], bool, Optional[float]) -> None
        pass

    @staticmethod
    @abstractmethod
    def _reserveOne(accessmanager, value, blocking, timeout=None):
        # type: (AccessManager[V], V, bool, Optional[float]) -> None
        pass

    @staticmethod
    @abstractmethod
    def _unreserveAll(accessmanager, *values):
        # type: (AccessManager[V], *V) -> None
        pass

    @staticmethod
    @abstractmethod
    def _unreserveOne(accessmanager, value):
        # type: (AccessManager[V], V) -> None
        pass

    def __enter__(self):
        self._with_started = True
        self._reserveAll(self._accessmanager, *self._unreserved_values, blocking=self._blocking, timeout=self._timeout)
        self._reserved_values.extend(self._unreserved_values)
        self._unreserved_values.clear()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._with_started = False
        self.unreserveAll()
        if exc_type:
            raise

    def addOneValue(self, value):
        if self._with_started:
            raise RuntimeError("Cannot add a value to a MassReserver from inside a with statement, use reserveOne()")
        self._unreserved_values.append(value)

    def addAllValues(self, *values):
        if self._with_started:
            raise RuntimeError("Cannot add values to a MassReserver from inside a with statement, use reserveOne()")
        self._unreserved_values.extend(values)

    def reserveAll(self, *values):
        # type: (*V) -> None
        """
        Reserves all additional values at the given AccessManager.
        Use this method to add Values from inside a with-Statement
        """
        self._unreserved_values.extend(values)
        self._reserveAll(self._accessmanager, *self._unreserved_values, blocking=self._blocking, timeout=self._timeout)
        self._reserved_values.extend(self._unreserved_values)
        self._unreserved_values.clear()

    def reserveOne(self, value):
        # type: (V) -> None
        """
        Reserves one additional value at the given AccessManager.
        Use this method to add a Value from inside a with-Statement
        """
        self._reserveOne(self._accessmanager, value, blocking=self._blocking, timeout=self._timeout)
        self._reserved_values.append(value)

    def unreserveAll(self):
        """
        Unreserves all remaining values at the given AccessManager
        """
        self._unreserveAll(self._accessmanager, *self._reserved_values)
        self._reserved_values.clear()

    def unreserveOne(self, value):
        # type: (V) -> None
        """
        Unreserves one value at the given AccessManager.
        If the given value was passed multiple times during initialisation, only the first occurance will be unreserved.

        A value that was unreserved with unreserveOne() will not get unreserved again with unreserveAll(), except it
        was passed multiple times during initialisation.
        """
        self._unreserveOne(self._accessmanager, value)
        self._reserved_values.remove(value)

    def listReservedValues(self):
        # type: () -> List[V]
        """
        Returns a list of currently reserved Values.
        """
        return list(self._reserved_values)
