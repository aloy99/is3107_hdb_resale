from datetime import datetime, timedelta
import logging
from typing import Mapping, Generator
import pandas as pd

from scraper.datagov.datagov_scraper import DatagovScraper
from scraper.datagov.constants import (
    DATAGOV_PROD_URL,
    DATASETS_META_ENDPOINT,
    RESALE_PRICE_COLLECTION_ID,
    RESALE_PRICE_FIELDS,
)

logger = logging.getLogger(__name__)

class ResalePriceScraper(DatagovScraper):

    def __init__(self, headers: Mapping[str, str], mode: str):
        super().__init__(headers)
        self.mode = mode

    def scrape_dataset(self, dataset_id: str) -> pd.DataFrame:
        file_path = self.download_data(dataset_id)
        df = pd.read_csv(file_path)
        cleaned_df = self.clean_df(df)
        return cleaned_df


    def clean_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.conform_dataframe_columns(df, RESALE_PRICE_FIELDS)
        df = df[df['month'] >= '2009-01']
        return df
    
    
    def run_scrape(self, current_date: datetime):
        if self.mode == 'backfill':
            return self.run_scrape_backfill()
        else:
            return self.run_scrape_live(current_date)

    def run_scrape_backfill(self) -> Generator[pd.DataFrame, None, None]:
        dataset_ids = self.get_dataset_ids(RESALE_PRICE_COLLECTION_ID)
        for dataset_id in dataset_ids:
            logger.info(f"Downloading dataset {dataset_id}")
            yield self.scrape_dataset(dataset_id)

    def run_scrape_live(self, current_date: datetime) -> Generator[pd.DataFrame, None, None]:
        curr_month_str = current_date.strftime("%Y-%m")
        prev_month_str = (current_date.replace(day=1) - timedelta(days=1)).strftime("%Y-%m")
        dataset_ids = self.get_dataset_ids(RESALE_PRICE_COLLECTION_ID)
        for dataset_id in dataset_ids:
            dataset_meta_response = self.get_req(
                DATAGOV_PROD_URL,
                DATASETS_META_ENDPOINT.format(dataset_id),
                {})
            if "onwards" in dataset_meta_response.json().get("data", {}).get("name", {}):
                live_dataset_found = True
                logger.info(f"Downloading dataset {dataset_id}")
                df =  self.scrape_dataset(dataset_id)
                df = df[df['month'].isin([curr_month_str, prev_month_str])]
                yield df
        if not live_dataset_found:
            logger.error("Live dataset not found in collection {RESALE_PRICE_COLLECTION_ID}, check DataGov website.")


