from pandas.io.parsers import ParserBase
from ..sentencizer import CustomSentencizer
from ..kairntech import Annotator, KairntechClient
import pandas as pd
import uuid
import json
from typing import Any, Callable, Dict, List, Union
from .recoder import Recoder
from .meta_column import MetaColumn
from ..processors import processors



class FileProcessor():

    @staticmethod
    def deserialize(params)->Dict:
        metas = []
        for meta in params["meta_columns"]:
            metas.append(MetaColumn.deserialize(meta))
        recoders = []
        for recoder in params["recoders"]:
            recoders.append(Recoder.deserialize(recoder))
        return FileProcessor(
                        params["verbatim_column"],
                        params["date_column"],
                        params["id_column"],
                        metas,
                        recoders=recoders,
                        drop_empty_verbatim=params["drop_empty_verbatim"],
                        encoding = params["encoding"],
                        generate_id = params["generate_id"],
                        csv_separator=params["csv_separator"]
                        )



    def __init__(self,
                 verbatim_column:str,
                 date_column:str,
                 id_column:str,
                 meta_columns:List[MetaColumn],
                 recoders:List[Recoder]=[],
                 drop_empty_verbatim:bool=True,
                 encoding:str='utf-8',
                 generate_id=False, 
                 csv_separator=","):
        self.verbatim_column = verbatim_column
        self.date_column = date_column
        self.id_column = id_column
        self.meta_columns = meta_columns
        self.recoders = recoders
        self.drop_empty_verbatim = drop_empty_verbatim
        self.encoding = encoding
        self.generate_id = generate_id
        self.csv_separator = csv_separator

    def __rename_columns(self, original_columns):
        columns = []
        for c in original_columns:
            if c==self.id_column:
                columns.append("id")
            if c==self.date_column:
                columns.append("dateInterview")
            if c==self.verbatim_column:
                columns.append("text")
            elif c in [m.column_name for m in self.meta_columns]:
                meta_column = list(filter(lambda x: x.column_name==c,self.meta_columns))[0]
                if meta_column.rename_column:
                    columns.append(meta_column.new_column_name)
                else:
                    columns.append(meta_column.column_name)
        return columns


    def serialize(self):
        return {
            "verbatim_column": self.verbatim_column,
            "date_column": self.date_column,
            "id_column": self.id_column,
            "meta_columns": [m.serialize() for m in self.meta_columns],
            "recoders": [r.serialize() for r in self.recoders],
            "drop_empty_verbatim": self.drop_empty_verbatim,
            "encoding": self.encoding,
            "generate_id": self.generate_id,
            "csv_separator": self.csv_separator
        }
        
    def __generate_id(self):
        return str(uuid.uuid4())

    @property
    def __columns_to_keep(self):
        return [self.id_column, self.date_column, self.verbatim_column] + [m.column_name for m in self.meta_columns]


    def __get_dataframe_from_path(self,filepath)->pd.DataFrame:
        if filepath.split(".")[1] == "xlsx":
            data = pd.read_excel(filepath, parse_dates=[self.date_column], encoding=self.encoding)
        elif filepath.split(".")[1] == "csv":
            data = pd.read_csv(filepath, parse_dates=[self.date_column], encoding=self.encoding, sep=self.csv_separator)
        else :
            raise Exception("only csv and xlsx format are supported")
        if self.generate_id:
            data[self.id_column] = data[data.columns[0]].apply(lambda x: self.__generate_id())
        return data

    def format_file(self, filepath)->str:
        """
        Format the given file to json format
        """
        data = self.__get_dataframe_from_path(filepath)
        # keep only the good columns
        data = data[self.__columns_to_keep]
        # drop empty verbatim if needed
        if self.drop_empty_verbatim:
            data[self.verbatim_column].dropna(inplace=True)
        # fill na if needed
        for meta in self.meta_columns:
            if meta.fill_na:
                data[meta.column_name].fillna(meta.na_filler)
        # rename columns if needed
        data.columns = self.__rename_columns(data.columns)
        # apply recoders
        for recoder in self.recoders:
            new_column = recoder.replacement_column if recoder.replace_column else recoder.column_name
            data[new_column] = data[recoder.column_name].apply(lambda x: recoder.apply(x))
            if recoder.replace_column:
                data.drop(recoder.column_name, axis=1, inplace=True)
        return data.to_json(orient="records")

    

def consolidate_sentiment(results:list):
    consolidated = {f"{x['key']}||{x['tonality']}" for x in results}
    consolidated = [{"key": x[0], "tonality": x[1]}
                    for x in [x.split("||") for x in consolidated]]
    global_tonality=None
    for topic in consolidated:
        if global_tonality==None:
            global_tonality = topic["tonality"]
            continue
        if global_tonality!=topic["tonality"]:
            global_tonality="mixed"
            break
    return consolidated, global_tonality


def annnotate_sentiment_and_topic(doc: str,
                                  topic_annotator : Annotator,
                                  sentiment_annotator: Annotator,
                                  sentencize_doc=True):
    results = []
    if sentencize_doc:
        sentencizer = CustomSentencizer()
        sentences = sentencizer.sentencize(doc)
        for sentence in sentences:
            topics = topic_annotator.annotate(sentence)
            if len(topics):
                sentiment = sentiment_annotator.annotate(sentence)
                if not len(sentiment):
                    sentiment = None
                else:
                    sentiment = sentiment[0]
                for code in [{"key": t, "tonality": sentiment} for t in topics]:
                    results.append(code)
    else:
        topics = topic_annotator.annotate( doc)
        sentiment = sentiment_annotator.annotate(doc)
        if not len(sentiment):
            sentiment = None
        else:
            sentiment = sentiment[0]
        for code in [{"key": t, "tonality": sentiment} for t in topics]:
            results.append(code)
    return consolidate_sentiment(results)

    