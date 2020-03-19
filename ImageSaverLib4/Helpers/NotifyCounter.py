import threading
import time
from abc import ABC, abstractmethod
from threading import Lock, Event, RLock
from typing import TypeVar, Generic, Optional, Dict, List, Tuple, Union, Type, Any, Callable, Iterator

V = TypeVar('V')


class NotifyCounter(Generic[V]):

    def __init__(self, startvalue, lock=None):
        # type: (V, Optional[Lock()], int) -> None
        if not lock:
            lock = Lock()
        self.value = startvalue
        self.lock = lock
        self.increase_function = lambda x: x + 1
        self.decrease_function = lambda x: x - 1
        self.wait_events = {}  # type: Dict[V, Tuple[Event, int]]

    def __repr__(self):
        return self.__class__.__name__ + '(' + repr(self.value) + ')'

    def __enter__(self):
        self.lock.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.lock.release()

    def getWaitingEvents(self):
        # type: () -> int
        with self.lock:
            return sum((v[1] for v in self.wait_events.values()))

    def set(self, value):
        # type: (V) -> None
        # print("call set", value)
        with self.lock:
            # print("inside set lock")
            self.value = value
            # print("notify others with", self.value)
            self._notify(self.value)

    def get(self):
        # type: () -> V
        with self.lock:
            return self.value

    def increase(self):
        """
        applies the increase_function on the internal value (default += 1)
        """
        with self.lock:
            self.value = self.increase_function(self.value)
            self._notify(self.value)

    def decrease(self):
        """
        applies the decrease_function on the internal value (default -= 1)
        """
        with self.lock:
            self.value = self.decrease_function(self.value)
            self._notify(self.value)

    def waitFor(self, value):
        # type: (V) -> None
        """
        blocks until the given value is reached
        """
        # event = None
        with self.lock:
            if self.value == value:
                return
            else:
                event = self._waitfor(value)
        # release the lock for other threads, wait outside the lock for a value-change notification
        # print(threading.current_thread().name, "waits for", event)
        event.wait()

    def notify(self, value):
        # type: (V) -> None
        """
        Notify all waiting threads, which wait for the given value
        """
        with self.lock:
            self._notify(value)

    def notifyAll(self):
        """
        Notify all waiting threads which wait for any value
        """
        with self.lock:
            for key in list(self.wait_events.keys()):
                self._notify(key)

    def _waitfor(self, value):
        # type: (V) -> Event
        """
        registers a new wait value and returns an event, which triggers, if the value is reached
        """
        if value not in self.wait_events:
            event = Event()
            self.wait_events[value] = (event, 1)
        else:
            event, count = self.wait_events[value]
            self.wait_events[value] = (event, count + 1)
        return event

    def _notify(self, value):
        # type: (V) -> None
        if value in self.wait_events:
            event, _ = self.wait_events.pop(value)
            event.set()


class SingleNotifyCounter(NotifyCounter):
    def __init__(self, startvalue, lock=None):
        super().__init__(startvalue, lock)
        self.wait_events = {}  # type: Dict[V, List[Event]]

    def getWaitingEvents(self):
        # type: () -> int
        with self.lock:
            return sum((len(v) for v in self.wait_events.values()))

    def endWaitingFor(self, value):
        # notify counter that a waiting thread finished waiting.
        # another thread could
        with self.lock:
            self._notify(self.value)

    def _waitfor(self, value):
        if value not in self.wait_events:
            event = Event()
            self.wait_events[value] = [event, ]
        else:
            event = Event()
            # print("creating event", event, "for", threading.current_thread().name)
            self.wait_events[value].append(event)
        return event

    def _notify(self, value):
        if value in self.wait_events:
            # print(self.wait_events)
            event = self.wait_events[value].pop(0)
            # print("notify", event)
            if len(self.wait_events[value]) == 0:
                self.wait_events.pop(value)
            event.set()

    def notify(self, value, notify_all=False):
        """
        Notify one Thread, which waits for the given value

        :param notify_all: if True, notifies all threads which wait for given value
        """
        if notify_all:
            with self.lock:
                if value in self.wait_events:
                    for _ in range(len(self.wait_events[value])):
                        self._notify(value)

        else:
            super().notify(value)

    def notifyAll(self, notify_all=False):
        """
        Notify one Thread for each waiting value

        :param notify_all: if True, notifies all threads
        """
        if notify_all:
            with self.lock:
                for key in list(self.wait_events.keys()):
                    for _ in range(len(self.wait_events[key])):
                        self._notify(key)
        else:
            super().notifyAll()


class ExclusiveValueReserver(Generic[V]):
    def __init__(self):
        self.lock = Lock()
        self.reserved_values = {}  # type: Dict[V, Tuple[RLock, int]]

    def reserve(self, value):
        # type: (V) -> RLock
        """
        returns a RLock, which is blocked by the calling thread.
        """
        with self.lock:
            if value in self.reserved_values:
                lock, count = self.reserved_values[value]
                self.reserved_values[value] = (lock, value + 1)
            else:
                lock = RLock()
                self.reserved_values[value] = (lock, 1)
            lock.acquire()
        return lock

    def unreserve(self, value):
        # type: (V) -> None
        with self.lock:
            if value in self.reserved_values:
                lock, count = self.reserved_values[value]
                count -= 1
                if count == 0:
                    self.reserved_values.pop(value)
                else:
                    self.reserved_values[value] = (lock, count)


class AccessException(Exception):
    pass


class NonBlockingException(AccessException):
    pass


class TimeoutException(AccessException):
    pass


def _exc_lock_aquire(lock, blocking=True, timeout=None):
    # type: (Union[Lock, RLock], bool, Optional[float]) -> None
    if timeout is not None:
        if not lock.acquire(blocking, timeout):
            raise TimeoutException()
    else:
        if not lock.acquire(blocking=blocking):
            raise NonBlockingException()


class Access(object):
    NONE = 0
    PARALLEL = 1
    EXCLUSIVE = 2

    def __init__(self):
        self.mutex = Lock()
        # self.used_by_count = 0
        # who holds the exclusive_joiner lock,
        # can be one of multiple parallel access threads or one exclusive access thread
        self.used_by = []  # type: List[int]
        self.current_access_type = self.NONE
        self.exclusively_used = Lock()
        self.exclusive_joiner = RLock()
        self._name = "<" + self.__class__.__name__ + " object at " + hex(id(self)) + ">"

    def __repr__(self):
        with self.mutex:
            return self._name

    @property
    def name(self):
        # type: () -> str
        with self.mutex:
            return self._name

    @name.setter
    def name(self, value):
        # type: (Any) -> None
        with self.mutex:
            self._name = "<" + self.__class__.__name__ + " object for " + repr(value) + ">"

    def usedByAnybody(self):
        with self.mutex:
            return self.current_access_type is not self.NONE
            # if self.exclusive_joiner.acquire(blocking=False):
            #     used = len(self.used_by) == 0 and not self.exclusively_used.locked()
            #     self.exclusive_joiner.release()
            #     return used
            # else:
            #     return True

    def parallelLeave(self, total_leave=False):
        with self.mutex:
            thread_ident = threading.get_ident()
            if total_leave:
                self.used_by = [i for i in self.used_by if i != thread_ident]
            else:
                self.used_by.remove(thread_ident)
            if len(self.used_by) == 0:
                # print(threading.current_thread().name, "releases exclusively_used", self._name)
                # self.used_by.clear()
                self.current_access_type = self.NONE
                self.exclusively_used.release()

    def exclusiveLeave(self):
        with self.mutex:
            thread_ident = threading.get_ident()
            if thread_ident not in self.used_by:
                raise RuntimeError("cannot release un-acquired lock")
            self.used_by.remove(thread_ident)
            if len(self.used_by) == 0:
                # print(threading.current_thread().name, "releases exclusively_used")
                self.current_access_type = self.NONE
                self.exclusively_used.release()
        self.exclusive_joiner.release()

    def parallelAccess(self, blocking=True, timeout=None):
        # short exclusivity
        # with self.exclusive_joiner:
        thread_ident = threading.get_ident()
        with self.mutex:
            if thread_ident in self.used_by:
                assert self.exclusively_used.locked()
                joiner_lock_needed = False  # reentrant parallel access
            else:
                joiner_lock_needed = True
        if joiner_lock_needed:
            _exc_lock_aquire(self.exclusive_joiner, blocking, timeout)
        try:
            with self.mutex:
                if len(self.used_by) == 0:
                    exclusively_used_lock_needed = True
                elif not self.exclusively_used.locked():
                    exclusively_used_lock_needed = True
                else:
                    exclusively_used_lock_needed = False
            if exclusively_used_lock_needed:
                try:
                    _exc_lock_aquire(self.exclusively_used, blocking, timeout)  # wait until not used
                except AccessException:
                    # with self.mutex:
                    # print(self._name)
                    # print(self.used_by)
                    # print(self.exclusively_used)
                    # print(self.exclusive_joiner)
                    raise
            with self.mutex:
                self.used_by.append(thread_ident)
                assert self.current_access_type in (self.NONE, self.PARALLEL)
                self.current_access_type = self.PARALLEL
        finally:
            if joiner_lock_needed:
                self.exclusive_joiner.release()

    def exclusiveAccess(self, blocking=True, timeout=None):
        # type: (bool, Optional[float]) -> None
        thread_ident = threading.get_ident()
        _exc_lock_aquire(self.exclusive_joiner, blocking, timeout)  # blocks parallel access
        with self.mutex:
            if len(self.used_by) == 0:
                exclusively_used_lock_needed = True
            elif thread_ident in self.used_by and self.current_access_type is self.EXCLUSIVE:
                exclusively_used_lock_needed = False
            else:
                exclusively_used_lock_needed = True
        if exclusively_used_lock_needed:
            _exc_lock_aquire(self.exclusively_used, blocking, timeout)  # wait until not used
        with self.mutex:
            self.used_by.append(thread_ident)
            assert self.current_access_type in (self.NONE, self.EXCLUSIVE)
            self.current_access_type = self.EXCLUSIVE


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
            self._values = list(values_gen)
        else:
            self._values = list(values)
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
        self._reserveAll(self._accessmanager, *self._values, blocking=self._blocking, timeout=self._timeout)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._with_started = False
        self.unreserveAll()
        if exc_type:
            raise

    def addOneValue(self, value):
        if self._with_started:
            raise RuntimeError("Cannot add a value to a MassReserver from inside a with statement, use reserveOne()")
        self._values.append(value)

    def addAllValues(self, *values):
        if self._with_started:
            raise RuntimeError("Cannot add values to a MassReserver from inside a with statement, use reserveOne()")
        self._values.extend(values)

    def reserveAll(self, *values):
        # type: (*V) -> None
        """
        Reserves all additional values at the given AccessManager.
        Use this method to add Values from inside a with-Statement
        """
        self._reserveAll(self._accessmanager, *values, blocking=self._blocking, timeout=self._timeout)
        self._values.extend(values)

    def reserveOne(self, value):
        # type: (V) -> None
        """
        Reserves one additional value at the given AccessManager.
        Use this method to add a Value from inside a with-Statement
        """
        self._reserveOne(self._accessmanager, value, blocking=self._blocking, timeout=self._timeout)
        self._values.append(value)

    def unreserveAll(self):
        """
        Unreserves all remaining values at the given AccessManager
        """
        self._unreserveAll(self._accessmanager, *self._values)

    def unreserveOne(self, value):
        # type: (V) -> None
        """
        Unreserves one value at the given AccessManager.
        If the given value was passed multiple times during initialisation, only the first occurance will be unreserved.

        A value that was unreserved with unreserveOne() will not get unreserved again with unreserveAll(), except it
        was passed multiple times during initialisation.
        """
        self._unreserveOne(self._accessmanager, value)
        self._values.remove(value)

    def listReservedValues(self):
        # type: () -> List[V]
        """
        Returns a list of currently reserved Values.
        """
        return list(self._values)


class ExclusiveMassReserver(MassReserver):
    @staticmethod
    def _reserveAll(accessmanager, *values, blocking, timeout=None):
        accessmanager.massExclusiveAccess(*values, blocking=blocking, timeout=timeout)

    @staticmethod
    def _reserveOne(accessmanager, value, blocking, timeout=None):
        accessmanager.exclusiveAccess(value, blocking=blocking, timeout=timeout)

    @staticmethod
    def _unreserveAll(accessmanager, *values):
        accessmanager.massExclusiveLeave(*values)

    @staticmethod
    def _unreserveOne(accessmanager, value):
        accessmanager.exclusiveLeave(value)


class ParallelMassReserver(MassReserver):
    @staticmethod
    def _reserveAll(accessmanager, *values, blocking, timeout=None):
        accessmanager.massParallelAccess(*values, blocking=blocking, timeout=timeout)

    @staticmethod
    def _reserveOne(accessmanager, value, blocking, timeout=None):
        accessmanager.parallelAccess(value, blocking=blocking, timeout=timeout)

    @staticmethod
    def _unreserveAll(accessmanager, *values):
        accessmanager.massParallelLeave(*values)

    @staticmethod
    def _unreserveOne(accessmanager, value):
        accessmanager.parallelLeave(value)


class AccessContext(ABC, Generic[V]):

    def __init__(self, accessmanager, value, blocking=True, timeout=None):
        # type: (AccessManager[V], V, bool, Optional[float]) -> None
        self._accessmanager = accessmanager
        self._value = value
        self._blocking = blocking
        self._timeout = timeout

    def __enter__(self):
        self.enter_func(self._accessmanager, self._value, self._blocking, self._timeout)

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


class ExclusiveAccessContext(AccessContext):

    @staticmethod
    def enter_func(accessmanager, value, blocking=True, timeout=None):
        accessmanager.exclusiveAccess(value, blocking=blocking, timeout=timeout)

    @staticmethod
    def exit_func(accessmanager, value):
        accessmanager.exclusiveLeave(value)


class ParallelAccessContext(AccessContext):
    @staticmethod
    def enter_func(accessmanager, value, blocking=True, timeout=None):
        accessmanager.parallelAccess(value, blocking=blocking, timeout=timeout)

    @staticmethod
    def exit_func(accessmanager, value):
        accessmanager.parallelLeave(value)
