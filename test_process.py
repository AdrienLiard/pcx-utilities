import unittest
from process import consolidate_sentiment


class TestProcess(unittest.TestCase):

    def test_consolidate_topic(self):
        result = [
            {"topic": "a", "sentiment": "positive"},
            {"topic": "a", "sentiment": "positive"},
            {"topic": "b", "sentiment": "negative"}
        ]
        consolidated = consolidate_sentiment(result)
        self.assertEqual(len(consolidated), 2)
        self.assertEqual(consolidated, [
            {"topic": "a", "sentiment": "positive"},
            {"topic": "b", "sentiment": "negative"}
        ])
