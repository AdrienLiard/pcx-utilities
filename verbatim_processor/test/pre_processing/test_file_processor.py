from verbatim_processor import processors
from verbatim_processor.pre_processing import FileFormater, MetaColumn
from verbatim_processor.pipeline.task import FileReader
from verbatim_processor.pipeline.task_runner import FileTaskRunner
from verbatim_processor.processors.processors import recod_nps
import unittest
import os
from pathlib import Path
import json


class TestFileFormater(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_path = Path(
            os.getcwd(), "verbatim_processor/test/fixtures/temp")
        self.test_file_path = str(
            Path(os.getcwd(), "verbatim_processor/test/fixtures/xl.xlsx"))

    def tearDown(self) -> None:
        for f in os.listdir(self.temp_path):
            os.remove(Path(self.temp_path, f))

    def test_run(self):
        fp = FileFormater("fp", "VERBATIM", "DATE_INTER", "ID", [])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        self.assertTrue(task_runner.is_task_completed(fp))

    def test_meta_column(self):
        """Test renaming a column"""
        meta = MetaColumn("Country", "Pays")
        fp = FileFormater("fp", "VERBATIM", "DATE_INTER", "ID", [meta])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        with open(task_runner.output_filename(fp), "r") as f:
            output = json.load(f)
        filters = [f for f in output[0]["data"]]
        self.assertTrue("Pays" in filters)
        self.assertTrue("text" in output[0])
        self.assertTrue("dateInterview" in output[0])
        self.assertTrue("id" in output[0])
        self.assertFalse("Country" in filters)
        self.assertFalse("Net Promoter Score" in filters)

