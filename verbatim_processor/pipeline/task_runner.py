from .task import Task, FileReader
from typing import List
from pathlib import Path
import logging


class TaskRunner():

    def __init__(self, name: str, **kwargs):
        self.name = name
        self.args = kwargs

    def output_filename(self, task):
        return self.name + "-" + task.name + "-" + "-".join([str(arg) for arg in self.args.values()]) + task.output_extension

    def is_task_completed(self, task):
        raise NotImplementedError

    def run_task(self, task: Task, input_task: List[Task] = []):
        raise NotImplementedError

    def check_input_tasks(input_tasks: List[Task]):
        raise NotImplementedError


class FileTaskRunner(TaskRunner):

    def __init__(self, name: str, folder: str, verbose=False, **kwargs):
        super().__init__(name, **kwargs)
        self.folder = folder
        self.verbose = verbose

    def is_task_completed(self, task: Task):
        return self.output_filename(task).exists()

    def output_filename(self, task):
        return Path(self.folder, super().output_filename(task))

    def check_input_tasks(self, input_tasks: List[Task]):
        for task in input_tasks:
            if not self.is_task_completed(task):
                return False
        return True

    def run_task(self, task: Task, input_tasks: List[Task] = []):
        if self.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        logging.info(f"Start running {task.name}")
        if self.is_task_completed(task):
            logging.info(f"Task {task.name} is already completed")
            return 0
        if self.check_input_tasks(input_tasks):
            task_output = task.run(*[self.output_filename(task)
                                     for task in input_tasks])
            if isinstance(task, FileReader):
                with open(self.output_filename(task), "wb") as f:
                    f.write(task_output)
            else:
                with open(self.output_filename(task), "w") as f:
                    f.write(task_output)
        else:
            logging.info(
                f"Task dependencies for {task.name} are not completed")
            return 0
        logging.info(f"Task {task.name} completed")
