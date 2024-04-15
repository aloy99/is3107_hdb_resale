from typing import Mapping
import requests
from scraper.arcgis.constants import RESERVOIRS_QUERY_URL
from pyproj import Transformer

from scraper.base_scraper import BaseScraper

class ReservoirScraper(BaseScraper):

    transformer = Transformer.from_crs("epsg:3414", "epsg:4326", always_xy=True)

    def __init__(self, headers: Mapping[str, str]):
        super().__init__("", "", headers)
        
    def get_reservoirs(self):
        url = RESERVOIRS_QUERY_URL
        # Make a GET request to fetch the data
        response = requests.get(url)
        # Check if the request was successful
        if response.status_code == 200:
            json_data = response.json()['features']
            data = []
            for r in json_data:
                long, lat = self.translate_coordinates(r['centroid']['x'], r['centroid']['y'])
                print(long)
                data.append({'name': r['attributes']['HYDRO_NAME'], 'lat': lat, 'long': long})
            return data
        else:
            print("Failed to retrieve data:", response.status_code)

    def translate_coordinates(self, x, y):
        return ReservoirScraper.transformer.transform(x,y)


    