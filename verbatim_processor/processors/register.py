import sys
from typing import Callable

def register(func:Callable, name:str):
  """Register a new processor in the processor modules

  Args:

  - func: Callable
  - name: name of the function

  .. code-block:: python
    func = lambda x: x*2
    register(func, "double")
    verbatims_processor.processors.double(1)
  """
  package = globals()["__package__"]
  setattr(sys.modules[package], name, func)
