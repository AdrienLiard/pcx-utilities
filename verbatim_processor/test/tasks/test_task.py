import unittest
from verbatim_processor.pipeline.task import Task, DummyTask, FileReader
from verbatim_processor.pipeline.task_runner import TaskRunner, FileTaskRunner
from pathlib import Path
import os


class TestTaskRunner(unittest.TestCase):

  def setUp(self) -> None:
      self.temp_path = Path(os.getcwd(),"verbatim_processor/test/fixtures/temp")
      self.test_file_path = Path(os.getcwd(),"verbatim_processor/test/fixtures/xl.xlsx")

  def tearDown(self) -> None:
      for f in os.listdir(self.temp_path):
        os.remove(Path(self.temp_path,f))

  def test_task_runner(self):
    """Test running a dummy task with base task runner, should raise a NotImplementedError"""
    task = DummyTask("test_task")
    task_runner = TaskRunner("runner", date="today", survey="me")
    with self.assertRaises(NotImplementedError):
      task_runner.run_task(task)

  def test_file_task_reader(self):
    """Test running a file reader task"""
    task = FileReader("file_reader_task", str(self.test_file_path))
    task_runner = FileTaskRunner("file_task_runner", folder=Path(self.temp_path), date="today", survey="me")
    task_runner.run_task(task)
    self.assertTrue(task_runner.is_task_completed(task))


  def test_file_task_reader_as_dep(self):
    """Test using a file reader task as a dep"""
    dep_task = FileReader("file_reader_task_dep", str(self.test_file_path))
    task = DummyTask("dummy task with dep")
    task_runner = FileTaskRunner("file_task_runner", folder=Path(self.temp_path), test="test")
    task_runner.run_task(task, [dep_task])
    self.assertFalse(task_runner.is_task_completed(task))
    task_runner.run_task(dep_task)
    task_runner.run_task(task, [dep_task])
    self.assertTrue(task_runner.is_task_completed(task))


  def test_check_file_output(self):
    """Test checking if is_task_completed"""
    task = DummyTask("test_task")
    task_runner = FileTaskRunner("runner", folder=Path(self.temp_path), date="today", survey="me")
    self.assertFalse(task_runner.is_task_completed(task))
    task_runner.run_task(task)
    self.assertTrue(task_runner.is_task_completed(task))

  def test_single_task_dependency(self):
    task = DummyTask("test_task")
    dependency_task = DummyTask("dep_task")
    task_runner = FileTaskRunner("runner", folder=Path(self.temp_path), date="today", survey="me")
    task_runner.run_task(task, [dependency_task])
    self.assertFalse(task_runner.is_task_completed(task))
    task_runner.run_task(dependency_task)
    self.assertTrue(task_runner.is_task_completed(dependency_task))
    task_runner.run_task(task, [dependency_task])
    self.assertTrue(task_runner.is_task_completed(task))