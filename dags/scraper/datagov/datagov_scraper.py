import logging
import json
from typing import List, Mapping
import backoff

import pandas as pd

from common.constants import DEV_MODE, DEV_REDUCED_ROWS
from scraper.base_scraper import BaseScraper
from scraper.datagov.constants import (
    DATAGOV_PROD_URL,
    DATAGOV_DATASETS_URL,
    DOWNLOAD_ENDPOINT,
    COLLECTIONS_ENDPOINT,
    DATASETS_META_ENDPOINT,
    DATASETS_ENDPOINT,
    DATAGOV_DOWNLOAD_HEADERS
)

logger = logging.getLogger(__name__)

class DatagovScraper(BaseScraper):

    def __init__(self, headers: Mapping[str, str]):
        super().__init__("", "", headers)

    @backoff.on_exception(
            backoff.expo,
            (KeyError, json.decoder.JSONDecodeError),
            max_tries=3)
    
    def download_data(self, dataset_id: str) -> str:
        url = DATAGOV_PROD_URL + DOWNLOAD_ENDPOINT.format(dataset_id)
        json_data = {}
        post_response = self.session.post(
            url,
            headers = DATAGOV_DOWNLOAD_HEADERS,
            json = json_data
        )
        download_url = post_response.json().get('data').get('url')
        download_path = dataset_id + '.csv'
        response = self.session.get(download_url, stream=True)
        with open(download_path, mode="wb") as file:
            for chunk in response.iter_content(chunk_size = 10 * 1024):
                file.write(chunk)
        return download_path
    
    def get_dataset_ids(self, collection_id: str) -> List[str]:
        response = self.get_req(DATAGOV_PROD_URL, COLLECTIONS_ENDPOINT.format(collection_id), {})
        collections_data = response.json() 
        try:
            dataset_ids = collections_data.get('data').get('collectionMetadata').get('childDatasets')
        except Exception:
            logger.exception(f"Unable to find child datasets in collection {collection_id}, check DataGov website.")
        return dataset_ids
    
    def conform_dataframe_columns(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        for col in columns:
            if col not in df.columns:
                df[col] = pd.Series()
        return df[columns]

