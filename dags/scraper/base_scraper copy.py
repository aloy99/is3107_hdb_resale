from abc import ABC, abstractmethod
from datetime import datetime
import logging
import os
from typing import Any, Mapping, Sequence
from pathlib import Path

import urllib3


logger = logging.getLogger(__name__)

class BaseScraper(ABC):

    def __init__(self, file_path: str, file_name: str, headers: Mapping[str, str]):
        self.headers = headers
        self.pool_manager = self.create_pool_manager()
        self.file_name = file_name
        self.file_path = Path(file_path)

    def create_pool_manager(self) -> urllib3.PoolManager:
        return urllib3.PoolManager()

    def get_req(self, base_url: str, endpoint: str, params: Sequence[Mapping[str,Any]]) -> urllib3.BaseHTTPResponse:
        try:
            response = self.pool_manager.request(
                'GET',
                base_url + endpoint,
                fields = params,
                headers = self.headers)
            return response
        except Exception as e:
            logger.exception(e)
        

    def get_file(self, file_url: str, params: Sequence[Mapping[str,Any]], date: datetime) -> Path:
        try:
            response = self.pool_manager.request(
                'GET',
                file_url,
                fields = params,
                headers = self.headers,
                preload_content = False)
        except Exception as e:
            logger.exception(e)

        output_path = self.generate_file_path(date)

        with open(output_path, 'wb') as file:
            while True:
                data = response.read(chunk_sie = 8192)
                if not data:
                    break
                file.write(data)
            
        return output_path
    
    def generate_file_path(self, date: datetime) -> Path:
        file_name = self.file_name.format(date.strftime("%Y%m%dT%H%M%S"))
        base, ext = os.path.splitext(file_name)
        file_path = Path.joinpath(self.file_path, file_name)
        idx = 1
        while file_path.exists():
            new_file_name = f"{base}_{idx}{ext}"
            file_path = Path.joinpath(self.file_path, new_file_name)
            idx += 1

        return file_path

    @abstractmethod
    def run_scrape(self):
        pass

    @abstractmethod
    def bs_parse(self):
        pass

    @abstractmethod
    def clean(self):
        pass


