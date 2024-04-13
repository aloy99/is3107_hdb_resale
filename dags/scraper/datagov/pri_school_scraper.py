import logging
from typing import Any, Mapping, Generator, Tuple, Sequence

import backoff

from scraper.base_scraper import BaseScraper
from scraper.datagov.constants import (
    DATAGOV_DATASETS_URL,
    DATASETS_ENDPOINT,
    PRIMARY_SCHOOL_DATASET_ID,
    PRIMARY_SCHOOL_FIELDS)

logger = logging.getLogger(__name__)

class PriSchoolScraper(BaseScraper):

    def __init__(self, headers: Mapping[str, str]):
        super().__init__("", "", headers)

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
                filtered_records = [self._row_handler(row) for row in records if row.get('mainlevel_code') == 'PRIMARY']
                if filtered_records:  # Only yield if there are any valid records
                    yield filtered_records

    
    def _row_handler(self, row: Mapping[str, Any]) -> Sequence[Any]:
        return tuple(row.get(field, None) for field in PRIMARY_SCHOOL_FIELDS)

    
    @backoff.on_exception(backoff.expo,
                           KeyError,
                           max_tries=3)
    def get_records(self, url: str, params: str) -> Tuple[Mapping[str, Any], Mapping[str, Any]]:
        response = self.get_req(url, "", params)
        data = response.json()
        return data, data['result']['records']
    
    def run_scrape(self):
        logger.info(f"Scraping dataset {PRIMARY_SCHOOL_DATASET_ID}")
        yield from self.scrape_dataset(PRIMARY_SCHOOL_DATASET_ID)