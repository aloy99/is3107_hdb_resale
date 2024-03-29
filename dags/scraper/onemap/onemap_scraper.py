from datetime import datetime
import logging
import os
from typing import Any, Mapping, Sequence, Generator
from pathlib import Path
import json

import pandas as pd

from scraper.base_scraper import BaseScraper
from scraper.onemap.constants import (
    ONEMAP_URL,
    SEARCH_ENDPOINT,
    OnemapSearchParams
)

logger = logging.getLogger(__name__)

class OnemapScraper(BaseScraper):

    def __init__(self, file_path: str, file_name: str, headers: Mapping[str, str]):
        super().__init__(file_path, file_name, headers)

    def scrape_landmark_coords(self, search_string: str) -> Mapping[str,Any]:
        response = self.get_req(ONEMAP_URL, SEARCH_ENDPOINT, vars(OnemapSearchParams(search_string)))
        try:
            data = response.json()
            total_pages = data['totalNumPages']
            results = data['results']
            while page_num <= total_pages:
                for result in results:
                    if result['SEARCHVAL'].lower() == search_string.lower():
                        print(result)
                        return {'Name': search_string, 'Latitude': result['LATITUDE'], 'Longitude': result['LONGITUDE']}
                page_num += 1
                response = self.get_req(ONEMAP_URL, SEARCH_ENDPOINT, vars(OnemapSearchParams(search_string, pageNum = page_num)))
                data = response.json()
                results = data['results']
            return {'Name': search_string, 'Latitude': None, 'Longitude': None}
        except ValueError:
            logger.info('JSONDecodeError')
            return {'Name': search_string, 'Latitude': None, 'Longitude': None}
        
    def scrape_address_coords(self, address: str) -> Mapping[str,Any]:
        response = self.get_req(ONEMAP_URL, SEARCH_ENDPOINT, vars(OnemapSearchParams("+".join(address.split(' ')))))
        try:
            data = response.json()
            return data['results']
        except ValueError:
            logger.info('JSONDecodeError')
            return {}

