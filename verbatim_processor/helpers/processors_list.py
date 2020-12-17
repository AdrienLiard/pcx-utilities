from ..processors import processors
from inspect import getmembers, isfunction

processors_list = [o[0] for o in getmembers(processors) if isfunction(o[1])]
