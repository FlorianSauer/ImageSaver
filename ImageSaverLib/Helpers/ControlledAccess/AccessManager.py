import time
from threading import Lock
from typing import Generic, Optional, Dict, List, Tuple, Type, Callable

from . import V
from .Access import Access
from .Errors import AccessException, NonBlockingException, TimeoutException


class AccessManager(Generic[V]):
    """
    Object to control the parallel and exclusive access to a value.
    Warning: The AccessManager does not work as an object storage, but more like a Lock, which controls the access to a
             Value.

    Parallel Access is the access to the same value by multiple threads in a controlled manner.
    During this access the value must not change.
    To ensure this, use the AccessManager with the parallelAccess() and parallelLeave() methods.
    A usage example would be parallel read operations on a shared resource.

    Exclusive Access is the access to the same value by only one thread at a time (like a classic Lock).
    An exclusive access would block until all parallel accesses are finished.
    During an exclusive access, other accesses (parallel or exclusive) will block.
    A usage example would be a write operation on a shared resource.


    """

    # Todo: move mass reservation to access manager, measure time between locking for better timeout

    # noinspection PyUnusedLocal
    def __init__(self, generic_type=None):
        # type: (Type[V]) -> None
        self.meta_lock = Lock()
        self.used_values = {}  # type: Dict[V, Tuple[int, Access]]  # counter, used, exclusively used, exclusive joiner
        self.debug = False

    def _create(self, value):
        # type: (V) -> Access
        if value not in self.used_values:
            access = Access()
            access.name = value
            self.used_values[value] = (1, access)
            if self.debug:
                print('creating Access object for value', repr(value))
        else:
            count, access = self.used_values[value]
            self.used_values[value] = (count + 1, access)
            if self.debug:
                print('recreating Access object for value', repr(value), 'and increasing count to', count+1)
        return access

    def _free(self, value):
        # type: (V) -> Access
        count, access = self.used_values[value]
        count -= 1
        if count == 0:
            self.used_values.pop(value)
            if self.debug:
                print('freeing up and removing Access object for value', repr(value))
        else:
            self.used_values[value] = (count, access)
            if self.debug:
                print('decreasing count for Access object with value', repr(value), "; new count is", count)
        return access

    def parallelAccess(self, value, blocking=True, timeout=None):
        # type: (V, bool, Optional[float]) -> None
        """
        Reserves a value for parallel access.
        Other threads can also access the value, threads which require exclusive access will block until all parallel
        access threads unreserved this value.

        Each parallelAccess() call must be followed by an parallelLeave() call (similar to an RLock)
        """
        with self.meta_lock:
            access = self._create(value)
        try:
            if self.debug:
                print('parallel accessing of Access object with value', repr(value), 'blocking='+repr(blocking), 'timeout='+repr(timeout))
            access.parallelAccess(blocking, timeout)
        except AccessException:
            with self.meta_lock:
                self._free(value)
            raise

    def exclusiveAccess(self, value, blocking=True, timeout=None):
        # type: (V, bool, Optional[float]) -> None
        """
        Reserves a value for exclusive access.
        No other thread can access this value.

        Each exclusiveAccess() call must be followed by an exclusiveLeave() call (similar to an RLock)
        """
        # print(threading.current_thread().name, threading.current_thread().ident, value)
        with self.meta_lock:
            access = self._create(value)
        try:
            if self.debug:
                print('exclusive accessing of Access object with value', repr(value), 'blocking='+repr(blocking), 'timeout='+repr(timeout))
            access.exclusiveAccess(blocking, timeout)
        except AccessException:
            with self.meta_lock:
                self._free(value)
            raise

    def parallelLeave(self, value):
        # type: (V) -> None
        """
        Un-reserves a value which was reserved for parallel access.
        Must be used with the parallelAccess() method to signal the AccessManager, that a parallel access to a value is
        now over.

        Unblocks a waiting exclusive Access.

        Each parallelAccess() call must be followed by an parallelLeave() call (similar to an RLock)
        """
        with self.meta_lock:
            access = self._free(value)
        if self.debug:
            print('parallel leaving of Access object with value', repr(value))
        access.parallelLeave()

    def exclusiveLeave(self, value):
        # type: (V) -> None
        """
        Un-reserves a value which was reserved for exclusive access.
        Must be used with the exclusiveAccess() method to signal the AccessManager, that a exclusive access to a value
        is now over.

        Unblocks a waiting parallel Access.

        Each parallelAccess() call must be followed by an parallelLeave() call (similar to an RLock)
        """
        with self.meta_lock:
            access = self._free(value)
        # try:
        if self.debug:
            print('exclusive leaving of Access object with value', repr(value))
        access.exclusiveLeave()
        # except ValueError as e:
        #     print(value)
        #     print(e)
        #     print(threading.current_thread().name)
        #     print(access.used_by, threading.current_thread().ident)
        #     raise

    def managedValues(self):
        # type: () -> List[V]
        with self.meta_lock:
            return list(self.used_values.keys())

    def managesValue(self, value):
        # type: (V) -> bool
        with self.meta_lock:
            return value in self.used_values

    def usedByAnybody(self, value):
        # type: (V) -> bool
        with self.meta_lock:
            return self.used_values[value][1].usedByAnybody()

    def accessCountForValue(self, value):
        # type: (V) -> int
        with self.meta_lock:
            return self.used_values[value][0]

    def __contains__(self, item):
        return self.managesValue(item)

    def massParallelAccess(self, *values, blocking=True, timeout=None):
        # type: (*V, bool, Optional[float]) -> None
        self._generic_mass_access(lambda a, b, t: a.parallelAccess(b, t),
                                  lambda a: a.parallelLeave(),
                                  *values,
                                  blocking=blocking,
                                  timeout=timeout)

    def massExclusiveAccess(self, *values, blocking=True, timeout=None):
        # type: (*V, bool, Optional[float]) -> None
        self._generic_mass_access(lambda a, b, t: a.exclusiveAccess(b, t),
                                  lambda a: a.exclusiveLeave(),
                                  *values,
                                  blocking=blocking,
                                  timeout=timeout)

    def massParallelLeave(self, *values):
        # type: (*V) -> None
        self._generic_mass_leave(lambda a: a.parallelLeave(), *values)

    def massExclusiveLeave(self, *values):
        # type: (*V) -> None
        self._generic_mass_leave(lambda a: a.exclusiveLeave(), *values)

    # Todo: add timeout+blocking to leave methods (for cache_meta lock)

    def _generic_mass_access(self, access_method, leave_method, *values, blocking=True, timeout=None):
        # type: (Callable[[Access, bool, Optional[float]], None], Callable[[Access], None], *V, bool, Optional[float]) -> None

        # sorting all given values resolves deadlocks.
        # thread t1 would want to access (v1, v2) and thread t2 would want to access (v2, v1).
        # t1 accesses v1, t2 accesses v2, t1 would now wait to access v2 and t2 would now wait to access v1
        _sorted_values = sorted(values)
        create_access_list = []  # type: List[Access]
        access_list = []  # type: List[Access]
        time_remaining = 0.0 if timeout is None else timeout
        with self.meta_lock:
            # Todo: i->value
            for i in range(len(_sorted_values)):
                access = self._create(_sorted_values[i])
                try:
                    access_method(access, False, None)
                    # print("AccessManager reserves", access, "within meta_lock nonblocking")
                    # access.exclusiveAccess(blocking=False)  # instantly try to lock it
                    create_access_list.append(access)
                except NonBlockingException:
                    access_list.append(access)  # failed, trying to do this later
        for i, access in enumerate(access_list):
            try:
                past = time.time()
                if timeout is None:
                    access_method(access, blocking, None)
                    # access.exclusiveAccess(blocking=blocking)
                else:
                    access_method(access, blocking, time_remaining)
                    # access.exclusiveAccess(blocking=blocking, timeout=time_remaining)
                # print("AccessManager reserves", access)
                time_remaining -= time.time() - past
            except (TimeoutException, NonBlockingException):
                # print("### failed", value, "@", i)
                with self.meta_lock:
                    for value in _sorted_values:
                        # do not leave here, some values might not be even reached yet
                        self._free(value)
                    for a in create_access_list:
                        # leave all Accesses that were aquired during init in a non blocking mode
                        leave_method(a)
                        # a.exclusiveLeave()
                    for j in range(0, i):
                        # leave all Accesses that were aquired prior to the error
                        leave_method(access_list[j])
                        # access_list[j].exclusiveLeave()
                raise

    def _generic_mass_leave(self, leave_method, *values):
        # type: (Callable[[Access], None], *V) -> None
        # print(repr(values))
        # try:
        _sorted_values = sorted(values)
        # except TypeError:
        #     print(repr(values))
        #     raise
        access_list = []
        with self.meta_lock:
            for value in _sorted_values:
                access_list.append(self._free(value))
        for access in access_list:
            leave_method(access)


