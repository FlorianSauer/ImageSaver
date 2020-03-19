import inspect
from abc import ABC, abstractmethod
from typing import Type, Dict

from .MetaDB import MetaDBInterface

from configparser import ConfigParser


def str_to_bool(s):
    # type: (str) -> bool
    if s.lower() in ('true', 't', 'on', 'yes'):
        return True
    elif s.lower() in ('false', 'f', 'off', 'no'):
        return False
    else:
        raise ValueError('Given value '+repr(s)+' cannot be parsed to boolean')


class MetaBuilderInterface(ABC):
    __meta_name__ = None

    @classmethod
    @abstractmethod
    def build(cls, **kwargs):
        # type: (**str) -> MetaDBInterface
        pass


class BuildError(Exception):
    pass


class MetaBuilder(object):
    META_SECTION_NAME = 'Meta'
    META_TYPE_OPTION_NAME = 'type'

    def __init__(self):
        self.classes = {}  # type: Dict[str, Type[MetaBuilderInterface]]

    def addMetaClass(self, meta_class):
        # type: (Type[MetaBuilderInterface]) -> None
        name = meta_class.__meta_name__ if meta_class.__meta_name__ else meta_class.__name__
        self.classes[name] = meta_class

    def build(self, meta_type, **parameters):
        # type: (str, **str) -> MetaDBInterface
        storage_class = self.classes[meta_type]
        return storage_class.build(**parameters)

    def build_from_config(self, parser, force_debug=False):
        # type: (ConfigParser, bool) -> MetaDBInterface
        if not parser.has_section(self.META_SECTION_NAME):
            raise BuildError("Section '" + self.META_SECTION_NAME + "' does not exist in config.")
        if not parser.has_option(self.META_SECTION_NAME, self.META_TYPE_OPTION_NAME):
            raise BuildError("Section '" + self.META_SECTION_NAME + "' does not have required type option '" + self.META_TYPE_OPTION_NAME + "'.")
        meta_name = parser.get(self.META_SECTION_NAME, self.META_TYPE_OPTION_NAME)
        if meta_name not in self.classes:
            raise BuildError("Cannot build Meta of type '"+meta_name+"'. Supported types: "+repr(self.classes.keys()))
        meta_class = self.classes[meta_name]
        parameters = {}  # type: Dict[str, str]
        for option in parser.options(self.META_SECTION_NAME):
            if option == self.META_TYPE_OPTION_NAME:
                continue
            parameters[option] = parser.get(self.META_SECTION_NAME, option)

        # check if parameters are given, which are not supported
        class_build_parameters = set(inspect.getfullargspec(meta_class.build).args)
        difference = set(parameters.keys()).difference(class_build_parameters)
        if difference:
            raise BuildError("Cannot build Meta of type '"+meta_name+"'. Meta does not support the following options: "+', '.join(difference))

        # check if required paremeters are missing
        sig = inspect.signature(meta_class.build)
        class_build_parameters = set([p.name for p in sig.parameters.values() if p.kind == p.POSITIONAL_OR_KEYWORD and p.default is p.empty])

        difference = class_build_parameters.difference(set(parameters.keys()))
        if difference:
            raise BuildError("Cannot build Meta of type '"+meta_name+"'. Meta is missing the following required options: "+', '.join(difference))
        if force_debug:
            parameters['echo'] = 'True'
        return meta_class.build(**parameters)
