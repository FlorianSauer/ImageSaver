class BaseImageSaverException(Exception):
    pass


class ConsistencyException(BaseImageSaverException):
    pass


class CompoundAlreadyExistsException(BaseImageSaverException):
    pass


class CompoundNotExistingException(BaseImageSaverException):
    pass


class FragmentMissingException(ConsistencyException):
    pass


class ResourceManipulatedException(ConsistencyException):
    pass


class ResourceMissingException(ConsistencyException):
    pass


class CompoundManipulatedException(ConsistencyException):
    pass


class FragmentManipulatedException(ConsistencyException):
    pass