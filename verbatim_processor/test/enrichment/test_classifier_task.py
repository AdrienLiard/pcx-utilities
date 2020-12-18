import unittest
from pathlib import Path
import os
import json
from verbatim_processor.pre_processing.file_processor import FileProcessor
from verbatim_processor.pipeline.task import FileReaderTask
from verbatim_processor.enrichment.sentencizer import CustomSentencizer
from verbatim_processor.pipeline.task_runner import FileTaskRunner
from verbatim_processor.enrichment.kairntech_task import KairntechTask
from verbatim_processor.kairntech import Annotator, KairntechClient
from verbatim_processor.enrichment.classifier import annnotate_sentiment_and_topic

class TestKairntechTask(unittest.TestCase):

  def setUp(self) -> None:
      self.temp_path = Path(os.getcwd(),"verbatim_processor/test/fixtures/temp")
      self.test_file_path = Path(os.getcwd(),"verbatim_processor/test/fixtures/xl.xlsx")
      self.test_file_path_csv = Path(os.getcwd(),"verbatim_processor/test/fixtures/csv.csv")

  def tearDown(self) -> None:
      for f in os.listdir(self.temp_path):
        os.remove(Path(self.temp_path,f))

  def test_annotate(self):
    client = KairntechClient("aliard", "ali123;", "https://sherpa.kairntech.com/api/")
    topic_annotator = Annotator(client, "bva_maaf_vide", "themes" )
    sentiment_annotator = Annotator(client, "bva_maaf_vide", "themes_2" )
    topic_annotator.annotate("Compétence, amabilité, compréhension")
    sentiment_annotator.annotate("Compétence, amabilité, compréhension, réactivité")

  def test_annotate_sentiment_and_topic(self):
    client = KairntechClient("aliard", "ali123;", "https://sherpa.kairntech.com/api/")
    topic_annotator = Annotator(client, "bva_maaf_vide", "themes" )
    sentiment_annotator = Annotator(client, "bva_maaf_vide", "themes_2")
    result = annnotate_sentiment_and_topic("Compétence, amabilité, compréhension, réactivité non égalée en ce contexte difficile pour tous", topic_annotator, sentiment_annotator)
    self.assertTrue(type(result[0]), list)
    self.assertTrue(type(result[1]), str)

  def test_run_kairntech_task(self):
    task_runner = FileTaskRunner("task_runner", self.temp_path)
    file_reader = FileReaderTask("xl reader", self.test_file_path)
    fp = FileProcessor("fp", "VERBATIM", "DATE_INTER", "ID", [])
    task_runner.run_task(file_reader)
    task_runner.run_task(fp, [file_reader])
    client = KairntechClient("aliard", "ali123;", "https://sherpa.kairntech.com/api/")
    topic_annotator = Annotator(client, "bva_maaf_vide", "themes" )
    sentiment_annotator = Annotator(client, "bva_maaf_vide", "themes_2")
    k_task = KairntechTask("k_task", topic_annotator, sentiment_annotator)
    task_runner.run_task(k_task, [fp])

  def test_run_kairntech_task_on_csv(self):
    sentencizer = CustomSentencizer()
    task_runner = FileTaskRunner("task_runner", self.temp_path)
    file_reader = FileReaderTask("xl reader", self.test_file_path_csv)
    fp = FileProcessor("fp", "Verbatim", "Date_Reponse", "id", [], [], encoding="ISO 8859-1", generate_id=True, csv_separator=";")
    task_runner.run_task(file_reader)
    task_runner.run_task(fp, [file_reader])
    client = KairntechClient("aliard", "ali123;", "https://sherpa.kairntech.com/api/")
    topic_annotator = Annotator(client, "bva_maaf_vide", "themes" )
    sentiment_annotator = Annotator(client, "bva_maaf_vide", "themes_2")
    k_task = KairntechTask("k_task", topic_annotator, sentiment_annotator, sentencizer)
    task_runner.run_task(k_task, [fp])
    self.assertTrue(task_runner.output_filename(k_task).exists())
    with open(task_runner.output_filename(k_task)) as f:
      result = json.load(f)
      for i in result:
        print(i)
    
