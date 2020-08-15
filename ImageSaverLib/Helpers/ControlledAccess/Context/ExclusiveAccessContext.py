from .AccessContext import AccessContext


class ExclusiveAccessContext(AccessContext):

    @staticmethod
    def enter_func(accessmanager, value, blocking=True, timeout=None):
        accessmanager.exclusiveAccess(value, blocking=blocking, timeout=timeout)

    @staticmethod
    def exit_func(accessmanager, value):
        accessmanager.exclusiveLeave(value)
