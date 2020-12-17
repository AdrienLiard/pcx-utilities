from typing import List
from pathlib import Path


class Task():

  def __init__(self, name):
    self.name = name
    self.output_extension = "txt"

  def run(self, input):
    raise NotImplementedError

class DummyTask(Task):

  def __init__(self, name):
    super().__init__(name)

  def run(self, *input):
    return self.name

class FileReaderTask(Task):

  def __init__(self, name, filename):
    super().__init__(name)
    self.filename = filename
    self.output_extension = Path(filename).suffix

  def run(self, *_input):
    with open(self.filename, "rb") as f:
      content = f.read()
    return content
   