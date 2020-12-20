from os import stat_result
from verbatim_processor.helpers.chunk import chunk
from verbatim_processor.pipeline.task import Task
import json
import requests
import logging
from verbatim_processor.helpers import chunk
import math


class PushPCXTask(Task):

    def __init__(self, name, token, survey_name, chunk_size=1000, pcx_api_url="https://collector.bva-cx-insights.com/api/1.0/verbatim/"):
        super().__init__(name)
        self.output_extension = ".json"
        self.pcx_api_url = pcx_api_url
        self.token = token
        self.survey_name = survey_name
        self.chunk_size = chunk_size

    def push_data(self, data):
        headers = {"Authorization": "Token " + self.token}
        r = requests.post(self.pcx_api_url, json=data, headers=headers)
        logging.info(
            f"Push to pcx ... Status code: {r.status_code} Text: {r.text}")
        if r.status_code != 200:
            logging.error(
                f"Push to PCX failed... Status code: {r.status_code} Text:{r.text}")
        return r.status_code, r.text

    def run(self, *input):
        with open(input[0]) as f:
            data = json.load(f)
        for row in data:
            row["surveyName"] = self.survey_name
        # the dataset is splits in chunks to not overstress the api
        # responses array will store the api responses for each chunk
        responses = []
        for subset in chunk(data, self.chunk_size):
            status_code, text = self.push_data(subset)
            responses.append(
                {"chunk": subset, "statusCode": status_code, "text": text})
        return json.dumps(responses)
