from typing import Any, Callable, Dict, List, Union
import json

class MetaColumn():
    
    def __init__(self,
                column_name:str,
                rename_column:Union[bool,str]=False,
                fill_na:Union[bool,Any]=False):
        """
        Initialize a new meta column

        Args:
        column_name : name of the target column in the dataset
        rename_column: False by default, to rename column provide the new column name
        fill_na : if not False na values will be replaced by the value provided
        """
        self.column_name = column_name
        if rename_column:
            self.rename_column = True
            self.new_column_name = rename_column
        else:
            self.rename_column = False
        if fill_na:
            self.fill_na = True
            self.na_filler = fill_na
        else:
            self.fill_na = False
