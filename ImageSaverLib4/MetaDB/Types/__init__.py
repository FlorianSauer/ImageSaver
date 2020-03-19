from sqlalchemy import Column
from sqlalchemy.orm.attributes import InstrumentedAttribute


def register_types_on_base():
    from .Compound import Compound as _
    from .CompoundFragmentMapping import CompoundFragmentMapping as _
    from .Fragment import Fragment as _
    from .FragmentResourceMapping import FragmentResourceMapping as _
    from .Resource import Resource as _


class ColumnPrinterMixin(object):
    def __repr__(self):
        cls = self.__class__
        attributes = [a for a in dir(cls) if not a.startswith('_')]  # filter private attributes
        attributes = [a for a in attributes if
                      type(getattr(cls, a)) in (Column, InstrumentedAttribute)]  # filter non Column attributes
        return '<' + cls.__name__ + ' object with ' + ', '.join(
            ((a + '=' + repr(getattr(self, a)) for a in attributes))) + '>'

    def __str__(self):
        cls = self.__class__
        attributes = [a for a in dir(cls) if not a.startswith('_')]  # filter private attributes
        attributes = [a for a in attributes if
                      type(getattr(cls, a)) in (Column, InstrumentedAttribute)]  # filter non Column attributes
        return '<' + cls.__name__ + ' object with ' + ', '.join(
            ((a + '=' + str(getattr(self, a)) for a in attributes))) + '>'
