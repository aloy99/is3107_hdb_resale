import logging
from typing import Any, Mapping, Generator
import pandas as pd

from scraper.base_scraper import BaseScraper
from scraper.datagov.datagov_scraper import DatagovScraper
from scraper.datagov.constants import (
    SUPERMARKET_DATASET_ID,
    SUPERMARKET_FIELDS)

logger = logging.getLogger(__name__)

class SupermarketScraper(DatagovScraper):

    def __init__(self, headers: Mapping[str, str]):
        super().__init__(headers)

    def scrape_dataset(self) -> pd.DataFrame:

        file_path = self.download_data(SUPERMARKET_DATASET_ID)
        df = pd.read_csv(file_path)
        cleaned_df = self.clean_df(df)

        return cleaned_df
    
    def clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.conform_dataframe_columns(df, SUPERMARKET_FIELDS)
        return df

    def run_scrape(self):
        logger.info("Scraping supermarket dataset")
        return self.scrape_dataset()