from abc import ABC, abstractmethod
from datetime import datetime
import logging
import os
from typing import Any, Mapping, Sequence
from pathlib import Path

from bs4 import BeautifulSoup
import urllib3


logger = logging.getLogger(__name__)

class BaseScraper(ABC):

    def __init__(self, url: str, file_path: Path, headers: Mapping[str, str]):
        self.base_url = url
        self.headers = headers
        self.pool_manager = self.create_pool_manager()
        self.file_path = file_path

    def create_pool_manager(self) -> urllib3.PoolManager:
        return urllib3.PoolManager()

    def get_html(self, endpoint: str, params: Sequence[Mapping[str|Any]]) -> urllib3.BaseHTTPResponse:
        try:
            response = self.pool_manager.request(
                'GET',
                self.base_url + endpoint,
                fields = params,
                headers = self.headers)
            data = response.data.decode('utf-8')
        except Exception as e:
            logger.exception(e)

        return data
            


    def get_file(self, endpoint: str, params: str) -> Path:
        try:
            response = self.pool_manager.request(
                'GET',
                self.base_url + endpoint,
                fields = params,
                headers = self.headers,
                preload_content = False)
        except Exception as e:
            logger.exception(e)

        output_path = self.generate_file_path()

        with open(output_path, 'wb') as file:
            while True:
                data = response.read(chunk_sie = 8192)
                if not data:
                    break
                file.write(data)
            
        return output_path
    
    def generate_file_path(self) -> Path:
        return Path()


    @abstractmethod
    def run_scrape(self):
        pass

    @abstractmethod
    def bs_parse(self):
        pass

    @abstractmethod
    def clean(self):
        pass


