from .client import KairntechClient
import requests

class Annotator():

    def __init__(self, client: KairntechClient, project_name: str, annotator_name: str):
        self.project_name = project_name
        self.annotator_name = annotator_name
        self.client = client

    def annotate(self, document: str):
        headers = self.client.__get_headers(self.client.token, "text/plain")
        r = requests.post(
            f"{self.client.api_url}projects/{self.project_name}/annotators/{self.annotator_name}/_annotate", headers=headers, data=document)
        results = []
        for c in r.json()["categories"]:
            if "label" in c:
                results.append(c["label"])
            else:
                results.append(c["labelName"])
        return results