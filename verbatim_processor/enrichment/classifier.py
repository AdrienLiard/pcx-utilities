from ..kairntech import KairntechClient, Annotator


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

