import threading
from threading import Lock, RLock

from typing import List, Union, Optional, Any

from .Errors import AccessException, NonBlockingException, TimeoutException


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