import spacy
from spacy.pipeline import Sentencizer


class CustomSentencizer():

  @staticmethod
  def __custom_seg(doc):
    length = len(doc)
    for index, token in enumerate(doc):
        if (token.text == ';' and index!=(length - 1)):
            doc[index+1].sent_start = True
    return doc

  def __init__(self):
    self.nlp = spacy.load("fr_core_news_sm")
    self.nlp.add_pipe(self.__custom_seg, before='parser')

  
  def sentencize(self, text):
    try :
        doc = self.nlp(text)
        return [str(s) for s in doc.sents]
    except Exception as e:
        return []
