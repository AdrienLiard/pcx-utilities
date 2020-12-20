from verbatim_processor import processors
from verbatim_processor.pre_processing import FileFormater, MetaColumn
from verbatim_processor.pipeline.task import FileReader
from verbatim_processor.pipeline.task_runner import FileTaskRunner
from verbatim_processor.processors.processors import recod_nps
from verbatim_processor.processors.recoder import Recoder
import unittest
import os
from pathlib import Path
import json


class TestRecoder(unittest.TestCase):

    def setUp(self) -> None:
        self.temp_path = Path(
            os.getcwd(), "verbatim_processor/test/fixtures/temp")
        self.test_file_path = str(
            Path(os.getcwd(), "verbatim_processor/test/fixtures/xl.xlsx"))

    def tearDown(self) -> None:
        for f in os.listdir(self.temp_path):
            os.remove(Path(self.temp_path, f))

    def test_recoding(self):
        """Test recoding a column"""
        meta = MetaColumn("Net Promoter Score")
        fp = FileFormater("fp", "VERBATIM", "DATE_INTER", "ID", [meta])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        recoder = Recoder("nps_recod", "Net Promoter Score",
                          recod_nps, in_place=True)
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        task_runner.run_task(recoder, [fp])
        with open(task_runner.output_filename(recoder), "r") as f:
            output = json.load(f)

        self.assertIn(output[0]["data"]["Net Promoter Score"], [
                      "Promoteur", "Neutre", "Détracteur"])

    def test_wrong_parameters(self):
        """You should not be able to create a recoder with in_place == True with no new_column_name"""
        self.assertRaises(Exception, Recoder("nps_recod", "Net Promoter Score",
                          recod_nps, True))
    
    def test_recoding_not_in_place(self):
        """Test recoding a column and creating new column"""
        meta = MetaColumn("Net Promoter Score")
        fp = FileFormater("fp", "VERBATIM", "DATE_INTER", "ID", [meta])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        recoder = Recoder("nps_recod", "Net Promoter Score",
                          recod_nps, False, "NPS")
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        task_runner.run_task(recoder, [fp])
        with open(task_runner.output_filename(recoder), "r") as f:
            output = json.load(f)
        self.assertIn(output[0]["data"]["NPS"], [
                      "Promoteur", "Neutre", "Détracteur"])
        self.assertIn("Net Promoter Score", output[0]["data"])

    def test_recoding__with_drop(self):
        """Test recoding a column and creating new column"""
        meta = MetaColumn("Net Promoter Score")
        fp = FileFormater("fp", "VERBATIM", "DATE_INTER", "ID", [meta])
        task_runner = FileTaskRunner("task_runner", self.temp_path)
        file_reader = FileReader("xl reader", self.test_file_path)
        recoder = Recoder("nps_recod", "Net Promoter Score",
                          recod_nps, False, "NPS", True)
        task_runner.run_task(file_reader)
        task_runner.run_task(fp, [file_reader])
        task_runner.run_task(recoder, [fp])
        with open(task_runner.output_filename(recoder), "r") as f:
            output = json.load(f)
        self.assertIn(output[0]["data"]["NPS"], [
                      "Promoteur", "Neutre", "Détracteur"])
        self.assertNotIn("Net Promoter Score", output[0]["data"])
