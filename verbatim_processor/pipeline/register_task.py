from .task import Task
from typing import List

class TaskItem():

  def __init__(self, name:str, task:Task, dependencies: List[str]):
      self.name = name
      self.task = Task
      self.dependencies = dependencies
