import requests
import json

class KairntechClient():

    @staticmethod
    def get_headers(token, content_type="application/json"):
        headers = {"accept": "application/json",
                   "Content-Type": content_type}
        headers["Authorization"] = "Bearer " + token
        return headers

    def __get_headers(self, token=None, content_type="application/json"):
        headers = {"accept": "application/json",
                   "Content-Type": content_type}
        if token:
            headers["Authorization"] = "Bearer " + token
        return headers

    def __login(self, email, password):
        headers = self.__get_headers()
        payload = json.dumps({"email": email, "password": password})
        r = requests.post(f"{self.api_url}auth/login",
                          headers=headers, data=payload)
        return r.json()["access_token"]

    def __init__(self, email, password, api_url=f"https://sherpa.kairntech.com/api/"):
        self.api_url = api_url
        self.token = self.__login(email, password)

    def get_projects(self):
        headers = self.__get_headers(self.token)
        r = requests.get(
            f"{self.api_url}projects?computeMetrics=false", headers=headers)
        results = r.json()
        return [x["name"] for x in results]

    def get_annotators(self, project_name):
        headers = self.get_headers(self.token)
        r = requests.get(
            f"{self.api_url}projects/{project_name}/annotators_by_type", headers=headers)
        results = r.json()
        if "learner" in results:
            return [(x["label"], x["name"])
                    for x in results["learner"]]
        else:
            return []



