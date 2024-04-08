import logging
from typing import Any, Mapping, Sequence, Generator
import threading
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from scraper.base_scraper import BaseScraper
from scraper.onemap.constants import (
    ONEMAP_URL,
    SEARCH_ENDPOINT,
    OnemapSearchParams
)

logger = logging.getLogger(__name__)

class OnemapScraper(BaseScraper):

    def __init__(self, headers: Mapping[str, str]):
        super().__init__("", "", headers)

    def scrape_landmark_coords(self, search_string: str) -> Mapping[str,Any]:
        response = self.get_req(ONEMAP_URL, SEARCH_ENDPOINT, vars(OnemapSearchParams(search_string)))
        fields = set(['LATITUDE','LONGITUDE'])
        try:
            data = response.json()
            total_pages = data['totalNumPages']
            results = data['results']
            while page_num <= total_pages:
                for result in results:
                    if result['SEARCHVAL'].lower() == search_string.lower():
                        return {k.lower():v for k,v in data['results'].items() if k in fields}
                page_num += 1
                response = self.get_req(ONEMAP_URL, SEARCH_ENDPOINT, vars(OnemapSearchParams(search_string, pageNum = page_num)))
                data = response.json()
                results = data['results']
            return {k.lower():None for k in fields}
        except ValueError:
            logger.info('JSONDecodeError')
            return {k.lower():None for k in fields}
        
    def scrape_address_postal_coords(self, address: str) -> Mapping[str,Any]:
        response = self.get_req(ONEMAP_URL, SEARCH_ENDPOINT, vars(OnemapSearchParams("+".join(address.split(' ')))))
        fields = set(['LATITUDE','LONGITUDE','POSTAL'])
        try:
            data = response.json()
            return {k.lower():v for k,v in data['results'][0].items() if k in fields}
        except (ValueError, IndexError):
            return {k.lower():None for k in fields}
        
    def enhance_resale_price(self, data: pd.DataFrame) -> pd.DataFrame:
        # if data.shape[0] == 0:
        #     return data
        new_data = data.copy()
        address_list = (new_data['block'] + ' ' + new_data['street_name']).to_list()
        with ThreadPoolExecutor(10) as executor:
            results = list(executor.map(self.scrape_address_postal_coords, address_list))
        new_data[['latitude', 'longitude', 'postal']] = pd.DataFrame(results, index=new_data.index) if results else None
        return new_data

