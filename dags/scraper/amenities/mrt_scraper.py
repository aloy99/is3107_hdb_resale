import requests
import pandas as pd
from scraper.amenities.constants import MRT_OPENING_DATES_URL
from scraper.onemap.onemap_scraper import OnemapScraper

def get_mrt_opening_dates():
    response = requests.get(MRT_OPENING_DATES_URL)
    if response.status_code == 200:
        data = response.json()
        print("Retrieved MRT data!\n", data)
        return data
    else:
        print(f"Failed to retrieve MRT opening dates: {response.status_code}")
        return None

def get_mrt_location(OnemapScraper):
    mrt_dates = get_mrt_opening_dates()
    mrts_df = pd.DataFrame(list(mrt_dates.items()), columns=['mrt', 'opening_date'])
    mrts_df['opening_date'] = pd.to_datetime(mrts_df['opening_date'], format='%d %B %Y')
    mrts_df[['latitude', 'longitude']] = mrts_df['mrt'].apply(
        lambda x: pd.Series(OnemapScraper.scrape_address_postal_coords(x))[['latitude', 'longitude']]
    )    
    missing_latitude = mrts_df[mrts_df['latitude'].isna()]
    if len(missing_latitude) == 0:
        print("All MRT location data found")
    else:
        print("The following MRTs are missing location data:")
        print(missing_latitude)
    return mrts_df
