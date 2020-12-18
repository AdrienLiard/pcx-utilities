import unittest
from pathlib import Path
import os
import json
import requests
from unittest.mock import Mock,patch
from verbatim_processor.pre_processing.file_processor import FileProcessor
from verbatim_processor.pipeline.task import FileReaderTask
from verbatim_processor.enrichment.sentencizer import CustomSentencizer
from verbatim_processor.pipeline.task_runner import FileTaskRunner
from verbatim_processor.enrichment.kairntech_task import KairntechTask
from verbatim_processor.kairntech import Annotator, KairntechClient, client
from verbatim_processor.enrichment.classifier import annnotate_sentiment_and_topic


class TestKairntechTask(unittest.TestCase):

    @patch("verbatim_processor.kairntech.client.requests.post")
    def test_annotate(self, mock_post):
        mock_post.return_value.json.return_value = {"access_token":"token"}
        payload = json.dumps({"email": "test", "password": "test"})
        client = KairntechClient('test','test', "http://mockurl.com")
        login_url = f"{client.api_url}auth/login"
        headers = client.get_headers()
        mock_post.assert_called_with(login_url, headers=headers, data=payload)
        

        topic_annotator = Annotator(client, "bva_maaf_vide", "themes")
        url = f"{client.api_url}projects/{topic_annotator.project_name}/annotators/{topic_annotator.annotator_name}/_annotate"
        headers = client.get_headers(client.token, content_type="text/plain")
        mock_post.return_value.json.return_value = {"categories":[{"label":"cat1"}]}
        topic = topic_annotator.annotate("text")
        self.assertEqual(topic,["cat1"])
        mock_post.assert_called_with(url, headers=headers, data="text".encode('utf-8'))
