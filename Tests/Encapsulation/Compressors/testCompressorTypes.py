import itertools
import os
import unittest
from typing import Callable, Type, Union

from ImageSaverLib.Encapsulation.Compressors.AutoCompressor import AutoCompressor
from ImageSaverLib.Encapsulation.Compressors.BaseCompressor import BaseCompressor
from ImageSaverLib.Encapsulation.Compressors.StackedCompressor import StackedCompressor
from ImageSaverLib.Encapsulation.Compressors.Types import *
from .testBasicCompressor import TestBasicCompressor


class TestCompressorTypes(unittest.TestCase):
    def makeCompressorTestClass(self, init_wrapper):
        # type: (Callable[[], Union[Type[BaseCompressor], BaseCompressor]]) -> TestBasicCompressor
        class _CompressorTestClass(TestBasicCompressor):
            def initWrapper(self):
                return init_wrapper()

        return _CompressorTestClass()

    def test_BZ2Compressor(self):
        self.makeCompressorTestClass(lambda: BZ2Compressor).test_compressing()

    def test_LZMACompressor(self):
        self.makeCompressorTestClass(lambda: LZMACompressor).test_compressing()

    def test_PassThroughCompressor(self):
        self.makeCompressorTestClass(lambda: PassThroughCompressor).test_compressing()

    def test_ZLibCompressor(self):
        self.makeCompressorTestClass(lambda: ZLibCompressor).test_compressing()

    def test_StackedCompressor(self):
        compressors = [
            BZ2Compressor,
            LZMACompressor,
            PassThroughCompressor,
            ZLibCompressor
        ]

        stacked_compressor = StackedCompressor(*compressors)
        self.assertEqual('-'.join(w.get_compressor_type() for w in compressors),
                         stacked_compressor.get_compressor_type())
        self.makeCompressorTestClass(lambda: stacked_compressor).test_compressing()

    def test_AutoCompressor(self):
        compressors = [
            BZ2Compressor,
            LZMACompressor,
            PassThroughCompressor,
            ZLibCompressor
        ]
        auto_compressor = AutoCompressor()
        for compressor in compressors:
            auto_compressor.addCompressor(compressor)
        for compressor in compressors:
            self.assertIs(compressor, auto_compressor.getStackedCompressor(compressor.get_compressor_type()))
        for compressors_combination in itertools.chain(
                *map(lambda x: itertools.combinations(compressors, x), range(0, len(compressors) + 1))):
            if len(compressors_combination) == 0:
                continue
            self.makeCompressorTestClass(lambda: auto_compressor.getStackedCompressor(list(compressors_combination)))
            test_data = os.urandom(TestBasicCompressor.test_data_size)
            wrapper_type = auto_compressor.getStackedCompressor(
                [w.get_compressor_type() for w in list(compressors_combination)]).get_compressor_type()
            self.assertEqual(test_data, auto_compressor.decompress(auto_compressor.compress(test_data, wrapper_type),
                                                                   wrapper_type))


if __name__ == '__main__':
    unittest.main()
