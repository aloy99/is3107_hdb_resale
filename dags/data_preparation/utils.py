import pandas as pd
from data_preparation.constants import REPLACE_FLAT_MODEL_VALUES

def clean_resale_prices_for_visualisation(df):
    # Calculate price per sqm
    if 'floor_area_sqm' in df.columns:
        df['price_per_sqm'] = df['resale_price'] / df['floor_area_sqm']

    # Standardise flat models
    if 'flat_model' in df.columns:
        df['flat_model'] = df['flat_model'].apply(lambda x: x.upper())
        df = df.replace({'flat_model': REPLACE_FLAT_MODEL_VALUES})
        df['flat_model'] = df['flat_model'].apply(lambda x: x.title())
        print("Unique flat models:\n", df['flat_model'].unique()) # ['Standard' 'New Generation' 'Model A' 'Maisonette' 'Apartment' 'Special', 'Multi Generation']

    # Convert lease commencement date to years
    if 'lease_commence_date' in df.columns:
        df['lease_commence_date'] = pd.to_datetime(df['lease_commence_date'], format='%Y')

    # Impute remaining lease and convert to num years
    if 'remaining_lease' in df.columns:
        def impute_remaining_lease(row):
            lease_val = row['remaining_lease']
            # 1. Convert string values to int
            if isinstance(lease_val, str):
                yearmonth = [int(s) for s in lease_val.split() if s.isdigit()]
                if len(yearmonth) > 1: # if there's year and month
                    years = yearmonth[0] + (yearmonth[1]/12)
                else: # if only year
                    years = yearmonth[0]
                row['remaining_lease'] = years
            # 2. Impute NaN values
            elif pd.isna(row['remaining_lease']):
                lease_commencement_year = int(row['lease_commence_date'].year)
                sale_year = row['transaction_month'].year
                sale_month = row['transaction_month'].month
                total_months = (sale_year - lease_commencement_year) * 12 + sale_month - 1
                remaining_years = 99 - (total_months / 12)
                row['remaining_lease'] = remaining_years
            return row
        df = df.apply(impute_remaining_lease, axis=1)
        print("Standarised and imputed remaining lease values.\n", df.info())
            
    # Returned cleaned data
    return df