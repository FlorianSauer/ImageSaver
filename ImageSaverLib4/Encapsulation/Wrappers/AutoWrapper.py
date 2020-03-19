from typing import Dict, Type, Union, List

from .BaseWrapper import BaseWrapper
from .BaseWrapperFactory import BaseWrapperFactory
from .StackedWrapper import StackedWrapper
from .Types import SVGWrapper, PNGWrapper, PNG3DWrapper, PassThroughWrapper, SizeChecksumWrapper, MinimumSizeWrapper
from . import WrappingType


class AutoWrapper(object):
    def __init__(self):
        self.wrapper_mappings = {}  # type: Dict[str, Type[BaseWrapper]]
        self.addWrapper(SVGWrapper)
        self.addWrapper(PNGWrapper)
        self.addWrapper(PNG3DWrapper)
        self.addWrapper(PassThroughWrapper)
        self.addWrapper(SizeChecksumWrapper)
        self.addWrapper(MinimumSizeWrapper(0))
        self.addWrapper(MinimumSizeWrapper(512))
        self.addWrapper(MinimumSizeWrapper(1000))  # 1 KB
        self.addWrapper(MinimumSizeWrapper(10000))  # 10 KB
        self.addWrapper(MinimumSizeWrapper(100000))  # 100 KB
        self.addWrapper(MinimumSizeWrapper(1000000))  # 1 MB
        self.addWrapper(MinimumSizeWrapper(10000000))  # 10 MB

    def resetContext(self):
        for wrapper in self.wrapper_mappings.values():
            if issubclass(type(wrapper), BaseWrapperFactory):
                wrapper = wrapper  # type: BaseWrapperFactory
                wrapper.resetContext()

    def addWrapper(self, wrapper):
        # type: (Union[Type[BaseWrapper], BaseWrapper]) -> None
        if '-' in wrapper.get_wrapper_type():
            raise TypeError("Wrapper type contains '-'.")
        self.wrapper_mappings[wrapper.get_wrapper_type()] = wrapper

    def getStackedWrapper(self, wrap_type):
        # type: (Union[WrappingType, List[WrappingType]]) -> Union[StackedWrapper, Type[BaseWrapper], BaseWrapper]
        if type(wrap_type) is list:
            wrap_types = [wt for wt in wrap_type]
        else:
            wrap_type = wrap_type.lower()
            wrap_types = wrap_type.split('-')
        wrappers = []
        for wt in wrap_types:
            if wt not in self.wrapper_mappings:
                raise UnsupportedWrapperType("not supported wrapper " + repr(wt))
            wrappers.append(self.wrapper_mappings[wt])
        if len(wrappers) == 1:
            return wrappers[0]
        else:
            assert len(wrappers) > 0
            return StackedWrapper(*wrappers)

    def wrap(self, data, wrap_type):
        # type: (bytes, WrappingType) -> bytes
        wrapper = self.getStackedWrapper(wrap_type)
        return wrapper.wrap(data)

    def unwrap(self, data, wrap_type):
        # type: (bytes, WrappingType) -> bytes
        unwrapper = self.getStackedWrapper(wrap_type)
        return unwrapper.unwrap(data)


class UnsupportedWrapperType(Exception):
    pass
