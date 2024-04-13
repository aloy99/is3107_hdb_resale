import logging
import requests
from typing import Any, Mapping, Generator

from scraper.base_scraper import BaseScraper
from scraper.datagov.constants import (
    DATAGOV_GEOJSON,
)
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

def extract_geojson_info(geojson):
    results = []
    for feature in geojson['features']:
        geom_type = feature['geometry']['type']
        coords = feature['geometry']['coordinates']
        if geom_type == 'Point' and len(coords) >= 2:
            longitude, latitude = coords[:2] 
            # Parse the HTML description to extract the name of the park
            description_html = feature['properties'].get('Description', '')
            soup = BeautifulSoup(description_html, 'html.parser')
            # Find all 'td' elements, the second 'td' contains the park name
            td_elements = soup.find_all('td')
            park_name = td_elements[0].text if len(td_elements) > 1 else 'Unnamed'
            results.append({'park': park_name, 'latitude': latitude, 'longitude': longitude})
        else:
            print(f"Unhandled geometry type or insufficient coordinates: {geom_type}")
    return results

class ParkScraper(BaseScraper):

    def __init__(self, headers: Mapping[str, str]):
        super().__init__("", "", headers)

    def get_parks(self) -> Generator[Mapping[str,Any], None, None]:
        url = DATAGOV_GEOJSON
        # Make a GET request to fetch the data
        response = requests.get(url)
        # Check if the request was successful
        if response.status_code == 200:
            geojson_data = response.json()
            return extract_geojson_info(geojson_data)
        else:
            print("Failed to retrieve data:", response.status_code)
    
 