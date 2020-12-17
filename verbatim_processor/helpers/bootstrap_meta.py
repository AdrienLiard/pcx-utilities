from ..pre_processing import MetaColumn
import pandas as pd
from typing import List


def bootstrap_meta_from_file(filepath, mandatory_columns: List, encoding="utf-8", csv_separator=","):
    if filepath.split(".")[1] == "xlsx":
        data = pd.read_excel(filepath, encoding=encoding, n_rows=2)
    elif filepath.split(".")[1] == "csv":
        data = pd.read_csv(filepath, encoding=encoding, sep=csv_separator, nrows=2)
    else:
        raise Exception("only csv and xlsx format are supported")
    data.drop(mandatory_columns, axis=1, inplace=True)
    return [MetaColumn(c).serialize() for c in data.columns]


def list_column_from_file(filepath, encoding="utf-8", csv_separator=","):
    if filepath.split(".")[1] == "xlsx":
        data = pd.read_excel(filepath, encoding=encoding, n_rows=2)
    elif filepath.split(".")[1] == "csv":
        data = pd.read_csv(filepath, encoding=encoding, sep=csv_separator, nrows=2)
    else:
        raise Exception("only csv and xlsx format are supported")
    return data.columns.tolist()
