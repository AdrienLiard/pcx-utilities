from verbatim_processor import processors
from verbatim_processor.pre_processing import FileProcessor, Recoder, MetaColumn
from verbatim_processor.pipeline.task import FileReader
from verbatim_processor.pipeline.task_runner import FileTaskRunner
from verbatim_processor.processors.processors import recod_nps
import unittest
import os
from pathlib import Path
import json


class TestFileProcessor(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_path = Path(
            os.getcwd(), "verbatim_processor/test/fixtures/temp")
        self.test_file_path = str(
            Path(os.getcwd(), "verbatim_processor/test/fixtures/xl.xlsx"))

    def tearDown(self) -> None:
        for f in os.listdir(self.temp_path):
            os.remove(Path(self.temp_path, f))

    def test_run(self):
        fp = FileProcessor("fp", "VERBATIM", "DATE_INTER", "ID", [])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        self.assertTrue(task_runner.is_task_completed(fp))

    def test_meta_column(self):
        """Test renaming a column"""
        meta = MetaColumn("Country", "Pays")
        fp = FileProcessor("fp", "VERBATIM", "DATE_INTER", "ID", [meta])
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

    def test_nps_recoder(self):
        """Test recoding a column without creating new column"""
        meta = MetaColumn("Net Promoter Score")
        recoder = Recoder("Net Promoter Score", recod_nps)
        fp = FileProcessor("fp", "VERBATIM", "DATE_INTER",
                           "ID", [meta], [recoder])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        with open(task_runner.output_filename(fp), "r") as f:
            output = json.load(f)
        filters = [f for f in output[0]["data"]]
        self.assertTrue("Net Promoter Score" in filters)


    def test_nps_recoder_replace_column(self):
        """Test recoding a column with column replacement"""
        meta = MetaColumn("Net Promoter Score")
        recoder = Recoder("Net Promoter Score", recod_nps, "NPS_recod")
        fp = FileProcessor("fp", "VERBATIM", "DATE_INTER",
                           "ID", [meta], [recoder])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        with open(task_runner.output_filename(fp), "r") as f:
            output = json.load(f)
        filters = [f for f in output[0]["data"]]
        self.assertTrue("Net Promoter Score" in filters)
        self.assertTrue("NPS_recod" in filters)


    def test_nps_recoder_replace_column(self):
        """Test recoding a column with column drop"""
        meta = MetaColumn("Net Promoter Score")
        recoder = Recoder("Net Promoter Score", recod_nps, "NPS_recod", True)
        fp = FileProcessor("fp", "VERBATIM", "DATE_INTER",
                           "ID", [meta], [recoder])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        with open(task_runner.output_filename(fp), "r") as f:
            output = json.load(f)
        filters = [f for f in output[0]["data"]]
        self.assertFalse("Net Promoter Score" in filters)
        self.assertTrue("NPS_recod" in filters)


    def test_inject_processor(self):
        """Test injecting a dummy processor"""
        meta = MetaColumn("Country")
        recoder = Recoder("Country", lambda x: "TEST", "dummy", True)
        fp = FileProcessor("fp", "VERBATIM", "DATE_INTER",
                           "ID", [meta], [recoder])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        with open(task_runner.output_filename(fp), "r") as f:
            output = json.load(f)
        self.assertTrue(output[0]["data"]["dummy"] == "TEST")
