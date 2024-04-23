import logging
from typing import Any, Mapping, Generator
import pandas as pd

from scraper.datagov.datagov_scraper import DatagovScraper
from scraper.datagov.constants import (
    PRIMARY_SCHOOL_DATASET_ID,
    PRIMARY_SCHOOL_FIELDS)

logger = logging.getLogger(__name__)

class PriSchoolScraper(DatagovScraper):

    def __init__(self, headers: Mapping[str, str]):
        super().__init__(headers)

    def scrape_dataset(self) -> pd.DataFrame:

        file_path = self.download_data(PRIMARY_SCHOOL_DATASET_ID)
        df = pd.read_csv(file_path)
        cleaned_df = self.clean_df(df)

        return cleaned_df
    
    def clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['mainlevel_code'] == 'PRIMARY']
        df = self.conform_dataframe_columns(df, PRIMARY_SCHOOL_FIELDS)
        return df

    def run_scrape(self):
        logger.info("Scraping primary school dataset")
        return self.scrape_dataset()