import itertools
import os
import unittest
from typing import Callable, Type, Union

import humanfriendly

from ImageSaverLib4.Encapsulation.Wrappers.AutoWrapper import AutoWrapper
from ImageSaverLib4.Encapsulation.Wrappers.BaseWrapper import BaseWrapper
from ImageSaverLib4.Encapsulation.Wrappers.StackedWrapper import StackedWrapper
from ImageSaverLib4.Encapsulation.Wrappers.Types import *
from .testBasicWrapper import TestBasicWrapper


class TestWrapperTypes(unittest.TestCase):
    def makeWrapperTestClass(self, init_wrapper):
        # type: (Callable[[], Union[Type[BaseWrapper], BaseWrapper]]) -> TestBasicWrapper
        class _WrapperTestClass(TestBasicWrapper):
            def initWrapper(self):
                return init_wrapper()

        return _WrapperTestClass()

    def test_AES256CTRWrapper(self):
        self.makeWrapperTestClass(lambda: AES256CTRWrapper(b'hello world 1234' * 2)).test_wrapping()

    def test_MinimumSizeWrapper(self):
        for size in [humanfriendly.parse_size('0B'),
                     humanfriendly.parse_size('10B'),
                     humanfriendly.parse_size('100B'),
                     humanfriendly.parse_size('1KB'),
                     humanfriendly.parse_size('10KB'),
                     humanfriendly.parse_size('100KB'),
                     humanfriendly.parse_size('1MB'),
                     humanfriendly.parse_size('10MB')]:
            self.makeWrapperTestClass(lambda: MinimumSizeWrapper(size)).test_wrapping()

    def test_PaddingWrapper(self):
        self.makeWrapperTestClass(lambda: PaddingWrapper(block_len=16, algo=PaddingWrapper.PKCS7)).test_wrapping()
        self.makeWrapperTestClass(lambda: PaddingWrapper(block_len=16, algo=PaddingWrapper.ISO7816)).test_wrapping()
        self.makeWrapperTestClass(lambda: PaddingWrapper(block_len=16, algo=PaddingWrapper.X923)).test_wrapping()

        self.makeWrapperTestClass(lambda: PaddingWrapper(block_len=32, algo=PaddingWrapper.PKCS7)).test_wrapping()
        self.makeWrapperTestClass(lambda: PaddingWrapper(block_len=32, algo=PaddingWrapper.ISO7816)).test_wrapping()
        self.makeWrapperTestClass(lambda: PaddingWrapper(block_len=32, algo=PaddingWrapper.X923)).test_wrapping()

        self.makeWrapperTestClass(lambda: PaddingWrapper(block_len=64, algo=PaddingWrapper.PKCS7)).test_wrapping()
        self.makeWrapperTestClass(lambda: PaddingWrapper(block_len=64, algo=PaddingWrapper.ISO7816)).test_wrapping()
        self.makeWrapperTestClass(lambda: PaddingWrapper(block_len=64, algo=PaddingWrapper.X923)).test_wrapping()

    def test_PaddingWrapperPayload(self):
        wrapper = PaddingWrapper()
        payload = os.urandom(TestBasicWrapper.test_data_size)
        self.assertIn(payload, wrapper.wrap(payload))

    def test_PassThroughWrapper(self):
        self.makeWrapperTestClass(lambda: PassThroughWrapper).test_wrapping()

    def test_PNGWrapper(self):
        self.makeWrapperTestClass(lambda: PNGWrapper).test_wrapping()

    def test_PNG3DWrapper(self):
        self.makeWrapperTestClass(lambda: PNG3DWrapper).test_wrapping()

    def test_SizeChecksumWrapper(self):
        self.makeWrapperTestClass(lambda: SizeChecksumWrapper).test_wrapping()

    def test_SizeChecksumWrapperPayload(self):
        wrapper = SizeChecksumWrapper
        payload = os.urandom(TestBasicWrapper.test_data_size)
        self.assertIn(payload, wrapper.wrap(payload))

    def test_SVGWrapper(self):
        self.makeWrapperTestClass(lambda: SVGWrapper).test_wrapping()

    def test_SVGWrapperPayload(self):
        wrapper = SVGWrapper
        payload = os.urandom(TestBasicWrapper.test_data_size)
        self.assertIn(payload.hex(), wrapper.wrap(payload).decode('utf-8'))

    def test_StackedWrapper(self):
        wrappers = [
            AES256CTRWrapper(b'hello world 1234' * 2),
            PaddingWrapper(),
            PassThroughWrapper,
            PNGWrapper,
            PNG3DWrapper,
            SizeChecksumWrapper,
            SVGWrapper
        ]

        stacked_wrapper = StackedWrapper(*wrappers)
        self.assertEqual('-'.join(w.get_wrapper_type() for w in wrappers), stacked_wrapper.get_wrapper_type())
        self.makeWrapperTestClass(lambda: stacked_wrapper).test_wrapping()

    def test_AutoWrapper(self):
        wrappers = [
            AES256CTRWrapper(b'hello world 1234' * 2),
            PaddingWrapper(),
            PassThroughWrapper,
            PNGWrapper,
            PNG3DWrapper,
            SizeChecksumWrapper,
            SVGWrapper
        ]
        auto_wrapper = AutoWrapper()
        for wrapper in wrappers:
            auto_wrapper.addWrapper(wrapper)
        for wrapper in wrappers:
            self.assertIs(wrapper, auto_wrapper.getStackedWrapper(wrapper.get_wrapper_type()))
        for wrappers_combination in itertools.chain(
                *map(lambda x: itertools.combinations(wrappers, x), range(0, len(wrappers) + 1))):
            if len(wrappers_combination) == 0:
                continue
            self.makeWrapperTestClass(lambda: auto_wrapper.getStackedWrapper(list(wrappers_combination)))
            test_data = os.urandom(TestBasicWrapper.test_data_size)
            wrapper_type = auto_wrapper.getStackedWrapper(
                [w.get_wrapper_type() for w in list(wrappers_combination)]).get_wrapper_type()
            self.assertEqual(test_data, auto_wrapper.unwrap(auto_wrapper.wrap(test_data, wrapper_type), wrapper_type))


if __name__ == '__main__':
    unittest.main()
