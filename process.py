from sentencizer import CustomSentencizer
from kairntech import KairntechClient, Annotator
import pandas as pd
    
    
def consolidate_sentiment(results):
    consolidated = {f"{x['topic']}||{x['sentiment']}" for x in results}
    consolidated = [{"topic": x[0], "sentiment": x[1]}
                    for x in [x.split("||") for x in consolidated]]
    return consolidated


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
                for code in [{"topic": t, "sentiment": sentiment} for t in topics]:
                    results.append(code)
    else:
        topics = topic_annotator.annotate( doc)
        sentiment = sentiment_annotator.annotate(doc)
        if not len(sentiment):
            sentiment = None
        else:
            sentiment = sentiment[0]
        for code in [{"topic": t, "sentiment": sentiment} for t in topics]:
            results.append(code)
    return consolidate_sentiment(results)
