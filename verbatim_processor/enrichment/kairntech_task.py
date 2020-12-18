from ..pipeline.task import Task
from ..kairntech import KairntechClient, Annotator
from ..enrichment.classifier import annnotate_sentiment_and_topic
import json

class KairntechTask(Task):

  def __init__(self, name, topic_annotator:Annotator, sentiment_annotator:Annotator, sentencizer=None):
      super().__init__(name)
      self.output_extension = "json"
      self.topic_annotator = topic_annotator
      self.sentiment_annotator = sentiment_annotator
      self.sentencizer = sentencizer

  def run(self, *input):
      input_file = input[0]
      with open(input_file, "r") as f:
        data = json.load(f)
      for row in data:
        classifier_result = annnotate_sentiment_and_topic(row["text"],
        self.topic_annotator,
        self.sentiment_annotator, self.sentencizer)
        if classifier_result[1]!=None:
          row["tonality"] = classifier_result[1].lower()
        if len(classifier_result[0]):
          row["themes"] = classifier_result[0]
      return json.dumps(data)
