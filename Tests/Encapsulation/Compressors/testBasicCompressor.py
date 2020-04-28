import os
import unittest
from abc import abstractmethod, ABC

import humanfriendly

from ImageSaverLib.Encapsulation import BaseCompressor


class TestBasicCompressor(unittest.TestCase, ABC):
    test_data_size = humanfriendly.parse_size('1 MB')

    @abstractmethod
    def initWrapper(self):
        # type: () -> BaseCompressor
        pass

    def test_compressing(self):
        wrap_wrapper = self.initWrapper()
        unwrap_wrapper = self.initWrapper()
        test_data = os.urandom(self.test_data_size)
        self.assertEqual(test_data, unwrap_wrapper.decompress(wrap_wrapper.compress(test_data)))
