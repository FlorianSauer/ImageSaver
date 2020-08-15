from .MassReserver import MassReserver


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
