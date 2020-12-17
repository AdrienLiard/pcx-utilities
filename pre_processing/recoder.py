from typing import Any, Callable, Dict, List, Union
import json
from ..processors import processors
import logging


class RecoderError(Exception):
    pass


class Recoder():

    def __init__(self,
                 column_name: str,
                 processor: Union[Callable, Dict],
                 replacement_column: Union[bool, str] = False,
                 drop_old_column: bool = False,
                 raise_errors: bool = False):
        """
        Recoders are useful to modify values in a column

        Args:
        column_name: name of the target column
        processor: A callable from the processors module or a Dict ({"old value": "new value"})
        replacement_column: False by default, if a string is provided a new column will be created with this name
        drop_old_column : False by default, if true and a replacement column is provided then the old column will be dropped
        raise_errors: False by default, if true the processor will raise an exception. If False the processor will return None for the value
        """
        self.column_name = column_name
        self.processor = processor
        self.raise_error = raise_errors
        self.drop_old_column = drop_old_column
        if type(self.processor) == dict:
            self.processor_type = "dict"
        else:
            self.processor_type = "func"
        if replacement_column:
            self.replacement_column = replacement_column
            self.replace_column = True
        else:
            self.replace_column = False
        
    def apply(self, value):
            if self.processor_type == "dict":
                if value in self.processor:
                    return self.processor[value]
                else:
                    logging.warning(f"Error in recoder {self.column_name}")
                    if self.raise_errors:
                        raise RecoderError(
                            f"{value} is not a valid value for the processor. Target column: {self.column_name}")
                    else:
                        return None
            try:
                return self.processor(value)
            except Exception as error:
                logging.warning(f"Error in recoder {self.column_name}")
                if self.raise_errors:     
                    raise error
                else:
                    return None

    def serialize(self):
        return {
            "column_name": self.column_name,
            "processor": self.processor.__name__ if self.processor_type=="func" else self.processor,
            "raise_error": self.raise_error,
            "replace_column": self.replacement_column if self.replace_column else self.replace_column,
            "drop_old_column": self.drop_old_column
        }

    @staticmethod
    def deserialize(dict:Dict):
        return Recoder(dict["column_name"],
                        getattr(processors,dict["processor"]) if type(dict["processor"])==str else dict["processor"],
                        dict["replace_column"],
                        dict["drop_old_column"],
                        dict["raise_error"]
                        )

        
