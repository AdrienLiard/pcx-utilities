from typing import Any, Callable, Dict, List, Union
import json
import logging
from verbatim_processor.pipeline.task import Task


class Recoder(Task):

    def __init__(self,
                name:str,
                 column_name: str,
                 processor: Union[Callable, Dict],
                 in_place: bool=True,
                 new_column_name: str=None,
                 drop_column:bool=False):
        """
        Recoders are useful to modify values in a column

        Args:
        column_name: name of the target column
        processor: A callable from the processors module or a Dict ({"old value": "new value"})
        in_place: if true the column will be replaced, new_column_name should then not be None
        new_column_name: new column will be created with this name, used only if in_place==False
        drop_column : if True the old column is dropped, used only if in_place==Fals
       
        """
        super().__init__(name)
        self.output_extension = ".json"
        self.column_name = column_name
        self.processor = processor
        self.in_place = in_place
        self.new_column_name = new_column_name
        self.drop_column = drop_column

        if in_place==False and new_column_name==None:
            raise Exception("A recoder can't have in_place==True without new_column_name")

        if type(self.processor) == dict:
            self.processor_type = "dict"
        else:
            self.processor_type = "func"
        
    def apply(self, value):
            if self.processor_type == "dict":
                if value in self.processor:
                    return self.processor[value]
                else:
                    logging.warning(f"Error in recoder {self.column_name} Value '{value}' not in dict")
                    return None
            try:
                return self.processor(value)
            except Exception as error:
                logging.warning(f"Error in recoder {self.column_name}")
                if self.raise_errors:     
                    raise error
                else:
                    return None 

    def run(self, *input):
        with open(input[0]) as f:
            data = json.load(f)
        for row in data:
            if self.in_place:
                row["data"][self.column_name] = self.apply(row["data"][self.column_name])
            elif self.drop_column==False:
                row["data"][self.new_column_name] = self.apply(row["data"][self.column_name])
            elif self.drop_column:
                value = row["data"].pop(self.column_name, None)
                if value!=None:
                    row["data"][self.new_column_name] = self.apply(value)
        return json.dumps(data)

