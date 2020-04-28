import inspect
from abc import ABC, abstractmethod
from configparser import ConfigParser
from typing import Type, Dict, List

import humanfriendly

from .StorageInterface import StorageInterface


def str_to_bool(s):
    # type: (str) -> bool
    if s.lower() in ('true', 't', 'on', 'yes'):
        return True
    elif s.lower() in ('false', 'f', 'off', 'no'):
        return False
    else:
        raise ValueError('Given value ' + repr(s) + ' cannot be parsed to boolean')


def str_to_bytesize(s):
    # type: (str) -> int
    return humanfriendly.parse_size(s)


class StorageBuilderInterface(ABC):
    __storage_name__ = None

    @classmethod
    @abstractmethod
    def build(cls, **kwargs):
        # type: (**str) -> StorageInterface
        pass


class BuildError(Exception):
    pass


class StorageBuilder(object):
    STORAGE_SECTION_NAME = 'Storage'
    STORAGE_TYPE_OPTION_NAME = 'type'

    def __init__(self):
        self.classes = {}  # type: Dict[str, Type[StorageBuilderInterface]]

    def addStorageClass(self, storage_class):
        # type: (Type[StorageBuilderInterface]) -> None
        # Todo: rename to __storage_name__
        name = storage_class.__storage_name__ if storage_class.__storage_name__ else storage_class.__name__
        self.classes[name.lower()] = storage_class

    def build(self, storage_name, parameters):
        # type: (str, Dict[str, str]) -> StorageInterface

        # check if parameters are given, which are not supported
        if storage_name not in self.classes:
            raise BuildError("Cannot build Storage of type '" + storage_name + "'.")
        storage_class = self.classes[storage_name]
        class_build_parameters = set(inspect.getfullargspec(storage_class.build).args)
        difference = set(parameters.keys()).difference(class_build_parameters)
        if difference:
            raise BuildError(
                "Cannot build Storage of type '" + storage_name + "'. Storage does not support the following options: " + ', '.join(
                    difference))

        # check if required paremeters are missing
        sig = inspect.signature(storage_class.build)
        class_build_parameters = set(
            [p.name for p in sig.parameters.values() if p.kind == p.POSITIONAL_OR_KEYWORD and p.default is p.empty])

        difference = class_build_parameters.difference(set(parameters.keys()))
        if difference:
            raise BuildError(
                "Cannot build Storage of type '" + storage_name + "'. Section is missing the following required options: " + ', '.join(
                    difference))

        return storage_class.build(**parameters)

    def build_from_args(self, args_dict):
        # type: (Dict[str, str]) -> StorageInterface
        if self.STORAGE_TYPE_OPTION_NAME not in args_dict:
            raise BuildError(
                "Missing required type option '" + self.STORAGE_TYPE_OPTION_NAME + "'.")
        storage_name = args_dict[self.STORAGE_TYPE_OPTION_NAME].lower()

        parameters = {}  # type: Dict[str, str]
        for option in args_dict.keys():
            if option == self.STORAGE_TYPE_OPTION_NAME:
                continue
            parameters[option] = args_dict[option]

        return self.build(storage_name, parameters)

    def build_from_config(self, parser):
        # type: (ConfigParser) -> StorageInterface
        # if not parser.has_section(self.STORAGE_SECTION_NAME):
        if not self._parser_has_storage_section(parser, self.STORAGE_SECTION_NAME):
            raise BuildError("Section '" + self.STORAGE_SECTION_NAME + "' does not exist in config.")
        for section in (s for s in parser.sections() if s.startswith(self.STORAGE_SECTION_NAME)):
            if not parser.has_option(section, self.STORAGE_TYPE_OPTION_NAME):
                raise BuildError(
                    "Section '" + self.STORAGE_SECTION_NAME + "' does not have required type option '" + self.STORAGE_TYPE_OPTION_NAME + "'.")
        section_name = self._parser_get_first_section(parser, self.STORAGE_SECTION_NAME)
        storage_name = parser.get(section_name, self.STORAGE_TYPE_OPTION_NAME).lower()
        parameters = {}  # type: Dict[str, str]
        for option in parser.options(section_name):
            if option == self.STORAGE_TYPE_OPTION_NAME:
                continue
            parameters[option] = parser.get(section_name, option)

        return self.build(storage_name, parameters)

    def _parser_has_storage_section(self, parser, starting_string):
        # type: (ConfigParser, str) -> bool
        return any(s.startswith(starting_string) for s in parser.sections())

    def _parser_get_first_section(self, parser, starting_string):
        # type: (ConfigParser, str) -> str
        for s in parser.sections():
            if s.startswith(starting_string):
                return s

    def build_all_from_config(self, parser):
        # type: (ConfigParser) -> List[StorageInterface]
        # if not parser.has_section(self.STORAGE_SECTION_NAME):
        if not self._parser_has_storage_section(parser, self.STORAGE_SECTION_NAME):
            raise BuildError("Section '" + self.STORAGE_SECTION_NAME + "' does not exist in config.")
        for section in (s for s in parser.sections() if s.startswith(self.STORAGE_SECTION_NAME)):
            if not parser.has_option(section, self.STORAGE_TYPE_OPTION_NAME):
                raise BuildError(
                    "Section '" + section + "' does not have required type option '" + self.STORAGE_TYPE_OPTION_NAME + "'.")
        storages = []
        for section_name in (s for s in parser.sections() if s.startswith(self.STORAGE_SECTION_NAME)):
            storage_name = parser.get(section_name, self.STORAGE_TYPE_OPTION_NAME).lower()
            parameters = {}  # type: Dict[str, str]
            for option in parser.options(section_name):
                if option == self.STORAGE_TYPE_OPTION_NAME:
                    continue
                parameters[option] = parser.get(section_name, option)

            storages.append(self.build(storage_name, parameters))
        if len(storages) == 0:
            raise BuildError('No storages built')
        return storages
