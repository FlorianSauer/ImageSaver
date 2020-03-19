import os
import unittest
from abc import abstractmethod, ABC

import humanfriendly

from ImageSaverLib4.Encapsulation import BaseWrapper


class TestBasicWrapper(unittest.TestCase, ABC):
    test_data_size = humanfriendly.parse_size('1 MB')

    @abstractmethod
    def initWrapper(self):
        # type: () -> BaseWrapper
        pass

    def test_wrapping(self):
        wrap_wrapper = self.initWrapper()
        unwrap_wrapper = self.initWrapper()
        test_data = os.urandom(self.test_data_size)
        self.assertEqual(test_data, unwrap_wrapper.unwrap(wrap_wrapper.wrap(test_data)))
