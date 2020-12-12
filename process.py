from sentencizer import CustomSentencizer
from kairntech import KairntechClient


def consolidate_sentiment(results):
    consolidated = {f"{x['topic']}||{x['sentiment']}" for x in results}
    consolidated = [{"topic": x[0], "sentiment": x[1]}
                    for x in [x.split("||") for x in consolidated]]
    return consolidated


def annnotate_sentiment_and_topic(kairntech_client: KairntechClient,
                                  doc: str,
                                  project_name: str,
                                  theme_annotator: str,
                                  sentiment_annotator: str,
                                  sentencize_doc=True):
    results = []
    if sentencize_doc:
        sentencizer = CustomSentencizer()
        sentences = sentencizer.sentencize(doc)
        for sentence in sentences:
            topics = kairntech_client.annotate(
                project_name, theme_annotator, sentence)
            if len(topics):
                sentiment = kairntech_client.annotate(
                    project_name, sentiment_annotator, sentence)
                if not len(sentiment):
                    sentiment = None
                else:
                    sentiment = sentiment[0]
                for code in [{"topic": t, "sentiment": sentiment} for t in topics]:
                    results.append(code)
    else:
        topics = kairntech_client.annotate(project_name, theme_annotator, doc)
        sentiment = kairntech_client.annotate(
            project_name, sentiment_annotator, doc)
        if not len(sentiment):
            sentiment = None
        else:
            sentiment = sentiment[0]
        for code in [{"topic": t, "sentiment": sentiment} for t in topics]:
            results.append(code)
    return results
