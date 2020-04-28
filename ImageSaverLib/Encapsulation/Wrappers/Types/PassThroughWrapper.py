from ..BaseWrapper import BaseWrapper


class PassThroughWrapper(BaseWrapper):

    _wrapper_type = 'pass'

    @classmethod
    def wrap(cls, data):
        return data

    @classmethod
    def unwrap(cls, data):
        return data
