import unittest
from verbatim_processor import processors
from verbatim_processor.pre_processing import Recoder
import verbatim_processor.processors.processors
from verbatim_processor.processors import register

class TestRecoder(unittest.TestCase):

  def test_deserialize(self):
    params = {
      "column_name": "test",
      "processor": "lowercase",
      "raise_error": False,
      "drop_old_column": False,
      "replace_column": False
    }
    Recoder.deserialize(params)

  def test_inject_processor(self):
    test_processor = lambda x: x
    register(test_processor, "test_processor")
    self.assertEqual(processors.test_processor(1),1)
    verbatim_processor.processors.test_processor = test_processor
    params = {
      "column_name": "test",
      "processor": "test_processor",
      "raise_error": False,
      "drop_old_column": False,
      "replace_column": False
    }
    Recoder.deserialize(params)