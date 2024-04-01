from abc import ABC, abstractmethod
from datetime import datetime
import logging
import os
from typing import Any, Mapping, Sequence
from pathlib import Path

import requests
import backoff


logger = logging.getLogger(__name__)

class BaseScraper(ABC):

    def __init__(self, file_path: str, file_name: str, headers: Mapping[str, str]):
        self.headers = headers
        self.session = self.create_session()
        self.file_name = file_name
        self.file_path = Path(file_path)

    def create_session(self) -> requests.Session:
        return requests.Session()


    @backoff.on_exception(backoff.expo,
                          (requests.exceptions.RequestException))
    def get_req(self, base_url: str, endpoint: str, params: Sequence[Mapping[str,Any]]) -> requests.Response:
        try:
            response = self.session.get(
                base_url + endpoint,
                params = params,
                headers = self.headers)
            return response
        except Exception as e:
            logger.exception(e)
        

    def get_file(self, file_url: str, params: Sequence[Mapping[str,Any]], date: datetime) -> Path:
        try:
            response = self.session.get(
                file_url,
                params = params,
                headers = self.headers)
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


