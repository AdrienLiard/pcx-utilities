import unittest
from pathlib import Path
import os
import json
import requests
from unittest.mock import Mock, patch
from verbatim_processor.pre_processing.file_processor import FileProcessor
from verbatim_processor.pipeline.task import FileReaderTask
from verbatim_processor.enrichment.sentencizer import CustomSentencizer
from verbatim_processor.pipeline.task_runner import FileTaskRunner
from verbatim_processor.enrichment.kairntech_task import KairntechTask
from verbatim_processor.kairntech import Annotator, KairntechClient, client
from verbatim_processor.enrichment.classifier import annnotate_sentiment_and_topic


class TestKairntechTask(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_path = Path(
            os.getcwd(), "verbatim_processor/test/fixtures/temp")
        self.test_file_path = Path(
            os.getcwd(), "verbatim_processor/test/fixtures/xl.xlsx")
        self.test_file_path_csv = Path(
            os.getcwd(), "verbatim_processor/test/fixtures/csv.csv")

    def tearDown(self) -> None:
        for f in os.listdir(self.temp_path):
            os.remove(Path(self.temp_path, f))

    @patch("verbatim_processor.kairntech.client.requests.post")
    def test_annotate(self, mock_post):
        mock_post.return_value.json.return_value = {"access_token": "token"}
        payload = json.dumps({"email": "test", "password": "test"})
        client = KairntechClient('test', 'test', "http://mockurl.com")
        login_url = f"{client.api_url}auth/login"
        headers = client.get_headers()
        mock_post.assert_called_with(login_url, headers=headers, data=payload)

        topic_annotator = Annotator(client, "bva_maaf_vide", "themes")
        url = f"{client.api_url}projects/{topic_annotator.project_name}/annotators/{topic_annotator.annotator_name}/_annotate"
        headers = client.get_headers(client.token, content_type="text/plain")
        mock_post.return_value.json.return_value = {
            "categories": [{"label": "cat1"}]}
        topic = topic_annotator.annotate("text")
        self.assertEqual(topic, ["cat1"])
        mock_post.assert_called_with(
            url, headers=headers, data="text".encode('utf-8'))

    @patch("verbatim_processor.kairntech.client.requests.post")
    def test_kairntech(self, mock_post):

        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReaderTask("xl reader", self.test_file_path)
        fp = FileProcessor("fp", "VERBATIM", "DATE_INTER", "ID", [])
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])

        mock_post.return_value.json.return_value = {"access_token": "token"}
        client = KairntechClient('test', 'test', "http://mockurl.com")

        mock_post.return_value.json.return_value = {
            "categories": [{"label": "cat1"}]}
        topic_annotator = Annotator(client, "project", "model")
        sentiment_annotator = Annotator(client, "project", "model")

        k_task = KairntechTask("k_task", topic_annotator, sentiment_annotator)
        task_runner.run_task(k_task, [fp])
        self.assertTrue(task_runner.output_filename(k_task).exists())

        with open(task_runner.output_filename(k_task)) as f:
            data = json.load(f)
            self.assertEqual(data[0]["tonality"], "cat1")
            self.assertEqual(data[0]["themes"], [
                             {"key": "cat1", "tonality": "cat1"}])

    @patch("verbatim_processor.kairntech.client.requests.post")
    def test_kairntech_on_csv_with_sentencizer(self, mock_post):

        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReaderTask("xl reader", self.test_file_path_csv)
        fp = FileProcessor("fp", "Verbatim", "Date_Reponse", "id", [
        ], encoding="ISO 8859-1", generate_id=True, csv_separator=";")
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])

        mock_post.return_value.json.return_value = {"access_token": "token"}
        client = KairntechClient('test', 'test', "http://mockurl.com")

        mock_post.return_value.json.return_value = {
            "categories": [{"label": "cat1"}]}
        topic_annotator = Annotator(client, "project", "model")
        sentiment_annotator = Annotator(client, "project", "model")

        sentencizer = CustomSentencizer()
        k_task = KairntechTask("k_task", topic_annotator,
                               sentiment_annotator, sentencizer)
        task_runner.run_task(k_task, [fp])
        self.assertTrue(task_runner.output_filename(k_task).exists())

        with open(task_runner.output_filename(k_task)) as f:
            data = json.load(f)
            self.assertEqual(data[0]["tonality"], "cat1")
            self.assertEqual(data[0]["themes"], [
                             {"key": "cat1", "tonality": "cat1"}])
