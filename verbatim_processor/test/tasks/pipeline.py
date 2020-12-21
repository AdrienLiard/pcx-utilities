import unittest
from verbatim_processor.pipeline.task import Task, DummyTask, FileReader
from verbatim_processor.pre_processing import FileFormater
from verbatim_processor.pipeline.pipeline import Pipeline
from verbatim_processor.pipeline.task_runner import TaskRunner, FileTaskRunner
from pathlib import Path
import os


class TestPipeline(unittest.TestCase):

  def setUp(self) -> None:
      self.temp_path = Path(os.getcwd(),"verbatim_processor/test/fixtures/temp")
      self.test_file_path = Path(os.getcwd(),"verbatim_processor/test/fixtures/xl.xlsx")

  def tearDown(self) -> None:
      for f in os.listdir(self.temp_path):
        os.remove(Path(self.temp_path,f))

  def test_run_pipeline(self):
    fp = FileFormater("fp", "VERBATIM", "DATE_INTER", "ID", [])
    task_runner = FileTaskRunner("task_runner", self.temp_path)
    file_reader = FileReader("xl reader", self.test_file_path)
    pipeline = Pipeline(task_runner, [file_reader, fp])
    pipeline.run()
    self.assertTrue(task_runner.output_filename(fp).exists())
    self.assertTrue(task_runner.output_filename(file_reader).exists())
    