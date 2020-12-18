import unittest
from pathlib import Path
import os
import json
from unittest import mock
from verbatim_processor import pcx
from verbatim_processor.pcx import PushPCXTask
from unittest.mock import Mock, patch
from verbatim_processor.pre_processing.file_processor import FileProcessor
from verbatim_processor.pipeline.task import FileReaderTask
from verbatim_processor.pipeline.task_runner import FileTaskRunner
import logging


class TestPCX(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_path = Path(
            os.getcwd(), "verbatim_processor/test/fixtures/temp")
        self.test_file_path = str(
            Path(os.getcwd(), "verbatim_processor/test/fixtures/xl.xlsx"))

    def tearDown(self) -> None:
        for f in os.listdir(self.temp_path):
            os.remove(Path(self.temp_path, f))

    @patch("verbatim_processor.pcx.push_pcx_task.chunk")
    @patch("verbatim_processor.pcx.push_pcx_task.requests.post")
    def test_pcx_push_task(self, mock_post, mock_chunk):
      mock_chunk.return_value = [[1,2,3],[1,2,3]]
      mock_post.return_value.status_code = 200
      mock_post.return_value.text = "ok"
      headers = {"Authorization": "Token " + "token"}
      task_runner = FileTaskRunner("task_runner", self.temp_path)
      file_reader = FileReaderTask("xl reader", self.test_file_path)
      fp = FileProcessor("fp", "VERBATIM", "DATE_INTER", "ID", [])
      pcx_task = PushPCXTask("push", "token", "survey_name")
      task_runner.run_task(file_reader)
      task_runner.run_task(fp, [file_reader])
      task_runner.run_task(pcx_task, [fp])
      mock_chunk.assert_called()
      mock_post.assert_called_with(pcx_task.pcx_api_url, headers=headers, json=[1,2,3])
      
