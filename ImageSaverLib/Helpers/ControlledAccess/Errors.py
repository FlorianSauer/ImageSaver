class AccessException(Exception):
    pass


class NonBlockingException(AccessException):
    pass


class TimeoutException(AccessException):
    pass
