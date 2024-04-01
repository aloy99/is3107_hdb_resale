import pandas as pd
from scraper.onemap.onemap_scraper import OnemapScraper



def enhance_resale_price(data: pd.DataFrame, scraper: OnemapScraper) -> pd.DataFrame:
    new_data = data.copy()
    new_data[['latitude', 'longitude']] = (new_data['block_num'] + ' ' + new_data['street_name']).transform(lambda x: pd.Series(scraper.scrape_address_postal_coords(x)))
    return new_data


