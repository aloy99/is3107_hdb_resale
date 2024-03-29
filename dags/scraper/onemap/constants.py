from dataclasses import dataclass

ONEMAP_URL = "https://www.onemap.gov.sg"
SEARCH_ENDPOINT = "/api/common/elastic/search?"

@dataclass
class OnemapSearchParams:
    searchVal: str
    returnGeom: str = "Y"
    getAddrDetails: str = "Y"
    pageNum: int = 1