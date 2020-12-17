import unittest
from ..pre_processing import consolidate_sentiment


class TestProcess(unittest.TestCase):

    def test_consolidate_topic(self):
        result = [
            {"key": "a", "tonality": "positive"},
            {"key": "a", "tonality": "positive"},
            {"key": "b", "tonality": "negative"}
        ]
        consolidated = consolidate_sentiment(result)
        self.assertEqual(len(consolidated[0]), 2)
        self.assertEqual(consolidated[1],"mixed")
        result = [
            {"key": "a", "tonality": "positive"},
            {"key": "a", "tonality": "positive"},
            {"key": "b", "tonality": "positive"}
        ]
        consolidated = consolidate_sentiment(result)
        self.assertEqual(consolidated[1],"positive")
        result = [
            {"key": "a", "tonality": "negative"},
            {"key": "a", "tonality": "negative"},
            {"key": "b", "tonality": "negative"}
        ]
        consolidated = consolidate_sentiment(result)
        self.assertEqual(consolidated[1],"negative")


