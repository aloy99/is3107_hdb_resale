import json
import logging
import pandas as pd
from typing import Mapping
from urllib.request import Request,urlopen
from scraper.singstat.constants import CPI_SINGSTAT_RESOURCE_ID
from scraper.base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class SingstatScraper(BaseScraper):

    def __init__(self, headers: Mapping[str, str]):
        super().__init__("", "", headers)

    def load_cpi_data(self):
        hdr = {'User-Agent': 'Mozilla/5.0', "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8"}
        url = f"https://tablebuilder.singstat.gov.sg/api/table/tabledata/{CPI_SINGSTAT_RESOURCE_ID}?seriesNoORrowNo=1"
        request = Request(url,headers=hdr)
        logger.info("Reading CPI data from singstat API...")
        data = json.loads(urlopen(request).read())
        cpi_data = data['Data']['row'][0]['columns']
        print("Converting to dataframe...")
        cpi_list = [{'month': item['key'], 'cpi': item['value']} for item in cpi_data]
        cpi_df = pd.DataFrame(cpi_list)
        print("Setting types...")
        cpi_df['month'] = pd.to_datetime(cpi_df['month'], format='%Y %b', errors='coerce').dt.date
        cpi_df['cpi'] = pd.to_numeric(cpi_df['cpi'], errors='coerce')
        print("\n\n CPI Dataframe retrieved:\n\n", cpi_df.head())
        return cpi_df