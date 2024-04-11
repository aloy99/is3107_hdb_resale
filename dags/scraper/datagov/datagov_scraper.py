from datetime import datetime, timedelta
import logging
from typing import Any, Mapping, Generator, Tuple, Sequence

import backoff

from common.constants import DEV_MODE, DEV_REDUCED_ROWS
from scraper.base_scraper import BaseScraper
from scraper.datagov.constants import (
    DATAGOV_COLLECTIONS_URL,
    DATAGOV_DATASETS_URL,
    COLLECTIONS_ENDPOINT,
    DATASETS_META_ENDPOINT,
    DATASETS_ENDPOINT,
    RESALE_PRICE_COLLECTION_ID,
    RESALE_PRICE_FIELDS)

logger = logging.getLogger(__name__)

class DataGovScraper(BaseScraper):

    def __init__(self, headers: Mapping[str, str], mode: str):
        super().__init__("", "", headers)
        self.mode = mode

    def scrape_dataset(self, dataset_id: str, params = {}) -> Generator[Mapping[str,Any], None, None]:
        url = DATAGOV_DATASETS_URL + DATASETS_ENDPOINT + f'?resource_id={dataset_id}'
        offset = 0
        total = 1
        while offset < total:
            data, records = self.get_records(url, params)
            offset = data['result'].get('offset', 0)
            total = data['result'].get('total', 0)
            url = DATAGOV_DATASETS_URL + data['result'].get('_links', {}).get('next').split("&filters")[0]

            if records:
                yield [self._row_handler(row) for row in records]
    
    def _row_handler(self, row: Mapping[str, Any]) -> Sequence[Any]:
        return tuple(row.get(field, None) for field in RESALE_PRICE_FIELDS)

    
    @backoff.on_exception(backoff.expo,
                           KeyError,
                           max_tries=3)
    def get_records(self, url: str, params: str) -> Tuple[Mapping[str, Any], Mapping[str, Any]]:
        response = self.get_req(url, "", params)
        data = response.json()
        return data, data['result']['records']
    
    def run_scrape(self, current_date: datetime):
        if self.mode == 'backfill':
            return self.run_scrape_backfill()
        else:
            return self.run_scrape_live(current_date)

    def run_scrape_backfill(self):
        params = {} if not DEV_MODE else {'limit': DEV_REDUCED_ROWS}
        response = self.get_req(DATAGOV_COLLECTIONS_URL, COLLECTIONS_ENDPOINT.format(RESALE_PRICE_COLLECTION_ID), params)
        collections_data = response.json()
        try:
            dataset_ids = collections_data.get('data').get('collectionMetadata').get('childDatasets')
        except Exception:
            logger.exception(f"Unable to find child datasets in collection {RESALE_PRICE_COLLECTION_ID}, check DataGov website.")
        for dataset_id in dataset_ids:
            logger.info(f"Scraping dataset {dataset_id}")
            yield from self.scrape_dataset(dataset_id)

    def run_scrape_live(self, current_date: datetime) -> Generator[Mapping[str,Any], None, None]:
        """
        Scrapes from the live dataset, for the current month and previous month.
        API does not support filter for GTE, so two queries are made.
        """
        curr_month_str = current_date.strftime("%Y-%m")
        prev_month_str = (current_date.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")    
        response = self.get_req(DATAGOV_COLLECTIONS_URL, COLLECTIONS_ENDPOINT.format(RESALE_PRICE_COLLECTION_ID), {})
        collections_data = response.json() 
        try:
            dataset_ids = collections_data.get('data').get('collectionMetadata').get('childDatasets')
        except Exception:
            logger.exception(f"Unable to find child datasets in collection {RESALE_PRICE_COLLECTION_ID}, check DataGov website.")
        live_dataset_found = False
        for dataset_id in dataset_ids:
            dataset_meta_response = self.get_req(
                DATAGOV_COLLECTIONS_URL,
                DATASETS_META_ENDPOINT.format(dataset_id),
                {})
            if "onwards" in dataset_meta_response.json().get("data", {}).get("name", {}):
                live_dataset_found = True
                yield from self.scrape_dataset(dataset_id, {'filters': f'{{"month": "{prev_month_str}"}}'})
                yield from self.scrape_dataset(dataset_id, {'filters': f'{{"month": "{curr_month_str}"}}'})
        if not live_dataset_found:
            logger.error("Live dataset not found in collection {RESALE_PRICE_COLLECTION_ID}, check DataGov website.")


