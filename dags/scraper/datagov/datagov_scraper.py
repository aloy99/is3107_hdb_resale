from datetime import datetime
import logging
import os
from typing import Any, Mapping, Sequence
from pathlib import Path
import json

import pandas as pd

from dags.scraper.base_scraper import BaseScraper
from dags.scraper.datagov.constants import (
    DATAGOV_COLLECTIONS_URL,
    DATAGOV_DATASETS_URL,
    COLLECTIONS_ENDPOINT,
    DATASETS_META_ENDPOINT,
    DATASETS_ENDPOINT,
    RESALE_PRICE_COLLECTION_ID)



logger = logging.getLogger(__name__)

class DataGovScraper(BaseScraper):

    def __init__(self, file_path: str, file_name: str, headers: Mapping[str, str], mode: str):
        super().__init__(file_path, file_name, headers)
        self.mode = mode


    def scrape_dataset(self, dataset_id: str):
        url = DATAGOV_DATASETS_URL + DATASETS_ENDPOINT + f'?resource_id={dataset_id}'
        offset = 0
        total = 1
        dataset = []
        while offset < total:
            response = self.get_req(url, "", {})
            data = response.json()
            dataset.extend(data.get('result').get('records'))
            offset = data['result'].get('offset', 0)
            total = data['result'].get('total', 0)
            url = DATAGOV_DATASETS_URL + data['result'].get('_links', {}).get('next')
        return dataset
    
    def run_scrape(self):
        if self.mode == 'backfill':
            return self.run_scrape_backfill()
        else:
            return self.run_scrape_live()


    def run_scrape_backfill(self):
        response = self.get_req(DATAGOV_COLLECTIONS_URL, COLLECTIONS_ENDPOINT.format(RESALE_PRICE_COLLECTION_ID), {})
        collections_data = response.json() 
        dataset_ids = collections_data.get('data').get('collectionMetadata').get('childDatasets')
        historical_data = []
        for dataset_id in dataset_ids:
            dataset = self.scrape_dataset(dataset_id)
            historical_data.append(pd.from_records(dataset))

        return pd.concat(historical_data)
            

    def run_scrape_live(self):
        response = self.get_req(DATAGOV_COLLECTIONS_URL, COLLECTIONS_ENDPOINT.format(RESALE_PRICE_COLLECTION_ID), {})
        collections_data = response.json() 
        dataset_ids = collections_data.get('data').get('collectionMetadata').get('childDatasets')
        for dataset_id in dataset_ids:
            dataset_meta_response = self.get_req(DATAGOV_COLLECTIONS_URL, DATASETS_META_ENDPOINT.format(dataset_id), {})
            breakpoint()
            if dataset_meta_response.json().get("data", {}).get("name", {}) == "Resale flat prices based on registration date from Jan-2017 onwards":
                dataset = self.scrape_dataset(dataset_id)
        try:
            return pd.from_records(dataset)
        except Exception:
            logger.exception("Unable to find live dataset")


