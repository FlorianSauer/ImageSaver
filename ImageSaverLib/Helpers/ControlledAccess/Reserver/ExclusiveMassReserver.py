from .MassReserver import MassReserver


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
