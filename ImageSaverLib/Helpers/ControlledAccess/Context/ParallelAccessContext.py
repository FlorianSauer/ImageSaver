from .AccessContext import AccessContext


class ParallelAccessContext(AccessContext):
    @staticmethod
    def enter_func(accessmanager, value, blocking=True, timeout=None):
        accessmanager.parallelAccess(value, blocking=blocking, timeout=timeout)

    @staticmethod
    def exit_func(accessmanager, value):
        accessmanager.parallelLeave(value)
