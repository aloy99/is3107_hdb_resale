import requests
import pandas as pd
from bs4 import BeautifulSoup
import requests
import pandas as pd
import re
from scraper.amenities.constants import MRT_OPENING_DATES_URL
from scraper.onemap.onemap_scraper import OnemapScraper

def get_mrt_opening_dates():
    response = requests.get(MRT_OPENING_DATES_URL)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        mrt_table = soup.find('table',{'class':"sortable"})
        df = pd.read_html(str(mrt_table))[0]
        df.columns = df.columns.droplevel(0)
        df = df.rename_axis(None, axis=1)
        df = df[df['English • Malay'] != df['Chinese']]
        df = df.rename(columns = {'English • Malay': 'mrt', 'Opening': 'opening_date'})[['mrt','opening_date']]
        df['mrt'] = df['mrt'].transform(lambda x: x.split('•')[0].strip())
        df['opening_date'] = df['opening_date'].transform(lambda x: pd.to_datetime(re.sub("\[[0-9]+\]", '', x), format="%d %B %Y", errors="coerce"))
        df = df.dropna(subset = ['opening_date'])
        df = df[df['opening_date'] < pd.Timestamp.now()]
        df['opening_date'] = df['opening_date'].dt.date
        return df
    else:
        print(f"Failed to retrieve MRT opening dates: {response.status_code}")
        return None

def get_mrts_location(OnemapScraper: OnemapScraper, mrts_df: pd.DataFrame):
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