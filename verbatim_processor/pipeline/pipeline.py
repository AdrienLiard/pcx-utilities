from .task import Reader, Task
from .task_runner import TaskRunner
from typing import List

class Pipeline():

  def __init__(self, task_runner: TaskRunner, tasks: List[Task]):
    self.task_runner = task_runner
    self.tasks = tasks

  def run(self):
    for i, task in enumerate(self.tasks):
      if isinstance(task,Reader):
        self.task_runner.run_task(self.tasks[i])
      else:
        self.task_runner.run_task(self.tasks[i], [self.tasks[i-1]])
