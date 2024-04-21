import pandas as pd
from sklearn.preprocessing import StandardScaler
from data_preparation.constants import REPLACE_FLAT_MODEL_VALUES
import time

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


def clean_resale_prices_for_ml(df):

    df = clean_resale_prices_for_visualisation(df)

    # label encode storeys
    df = df.sort_values(by='storey_range')
    df['storey_range'] = df['storey_range'].astype('category').cat.codes # label encode

    # remove flat types with very few cases
    df = df[~df['flat_type'].isin(['MULTI-GENERATION', 'MULTI GENERATION', '1 ROOM'])]

    # Re-categorize flat model to reduce num classes
    replace_values = {'Executive Maisonette':'Maisonette', 'Terrace':'Special', 'Adjoined flat':'Special',
                        'Type S1S2':'Special', 'DBSS':'Special', 'Model A2':'Model A', 'Premium Apartment':'Apartment', 'Improved':'Standard', 'Simplified':'Model A', '2-room':'Standard'}
    df = df.replace({'flat_model': replace_values})

    # Label encode flat type
    replace_values = {'2 ROOM':0, '3 ROOM':1, '4 ROOM':2, '5 ROOM':3, 'EXECUTIVE':4}
    df = df.replace({'flat_type': replace_values})
    d#f['flat_type'] = df['flat_type'].astype(int)
    # Change lease commence date type to scalable int
    df['lease_commence_date'] = df['lease_commence_date'].apply(lambda x: time.mktime(x.timetuple()))

    df = df.reset_index(drop=True)

    ## dummy encoding
    df = pd.get_dummies(df, columns=['flat_model'], prefix=['model'])
    df = df.drop('model_Standard',axis=1) # remove standard, setting it as the baseline

    scaler = StandardScaler()

    # fit to continuous columns and transform
    scaled_columns = ['floor_area_sqm','lease_commence_date','num_pri_sch_within_radius','num_mrt_within_radius','num_park_within_radius','num_supermarkets_within_radius']
    scaler.fit(df[scaled_columns])
    scaled_columns = pd.DataFrame(scaler.transform(df[scaled_columns]), index=df.index, columns=scaled_columns)

    # separate unscaled features
    unscaled_columns = df.drop(scaled_columns, axis=1)

    # concatenate scaled and unscaled features
    df = pd.concat([scaled_columns,unscaled_columns], axis=1)

    # Returned cleaned data
    return df