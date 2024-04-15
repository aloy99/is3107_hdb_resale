import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from datetime import datetime

import base64

from reporting.constants import IMAGE_PATHS, HTML_PATH, HTML_START, HTML_END, PLOT_TEMPLATE
from common.constants import PROXIMITY_RADIUS

def save_plot_as_image(plt, plot_name):
    plt.title(IMAGE_PATHS[plot_name]['title'])
    plt.tight_layout()
    plt.savefig( IMAGE_PATHS[plot_name]['path'])

def create_html_report():
    report_body = HTML_START.format(date = datetime.today().strftime('%Y-%m-%d')).strip()
    for item in IMAGE_PATHS.values():
        data_uri = base64.b64encode(open(item['path'], 'rb').read()).decode('utf-8')
        img_tag = '<img src="data:image/png;base64,{0}">'.format(data_uri)
        curr_row = PLOT_TEMPLATE.format(image = img_tag, caption = item['title'])
        report_body += curr_row
    report_body += HTML_END.strip()
    with open(HTML_PATH, 'w') as f:
        f.write(report_body.replace('\n',''))
    return report_body.replace('\n','')

def plot_default_features(df):

    def plot_real_prices(df: pd.DataFrame):
        _, ax = plt.subplots() 
        # Plot Unadjusted Prices
        df.groupby('transaction_month')['resale_price'].median().plot(ax=ax, color='#18bddd', label='Unadjusted for Inflation')
        # Plot Adjusted Prices
        df.groupby('transaction_month')['real_resale_price'].median().plot(ax=ax, color='#df9266', label='Adjusted for Inflation')
        # Format the x-axis to display dates nicely
        years = mdates.YearLocator()  # Every year
        years_fmt = mdates.DateFormatter('%Y')
        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(years_fmt)
        plt.xticks(rotation=45)  # Rotate x-ticks to prevent overlap
        # Set axis labels and title
        ax.set_xlabel('Date')
        ax.set_ylabel('Price in SGD ($)')
        # Set limits with padding
        x_min, x_max = ax.get_xlim()
        y_min, y_max = ax.get_ylim()
        padding_factor = 0.05
        x_padding = (x_max - x_min) * padding_factor
        y_padding = (y_max - y_min) * padding_factor
        ax.set_xlim(x_min - x_padding, x_max + x_padding)
        ax.set_ylim(y_min - y_padding, y_max + y_padding)
        # Legend and layout
        ax.legend()
        plt.tight_layout()
        # Save the plot
        save_plot_as_image(plt, 'real_prices')
        plt.close()

    def plot_floor_area_distribution(df: pd.DataFrame):
        plt.hist(df['floor_area_sqm'], bins=50, edgecolor='black')
        save_plot_as_image(plt, 'floor_area_distribution')
        plt.close()


    def plot_price_distribution_by_town(df):
        _, ax = plt.subplots()
        # Select top N towns for clarity
        top_towns = df['town'].value_counts().nlargest(10).index
        df_top_towns = df[df['town'].isin(top_towns)]
        # Map towns to numeric positions
        town_positions = {town: pos for pos, town in enumerate(sorted(top_towns))}
        df_top_towns['town_position'] = df_top_towns['town'].map(town_positions)
        # Create boxplot
        df_top_towns.boxplot(column='price_per_sqm', by='town_position', ax=ax)
        # Set labels
        ax.set_xticklabels([town for town in sorted(top_towns)], rotation=45, ha='right')
        ax.set_xlabel('Town')
        ax.set_ylabel('Price per Square Meter (SGD)')
        # Save figure
        plt.suptitle('')  # Suppress the automatic title
        save_plot_as_image(plt, 'price_distribution_by_town')
        plt.close()

    def plot_avg_price_per_sqm_by_town_flat_type(df):
        _, ax = plt.subplots() 
        # Select top N towns for clarity
        top_towns = df['town'].value_counts().nlargest(10).index
        df_top_towns = df[df['town'].isin(top_towns)]
        # Group by town and flat type, then calculate the average price per square meter
        grouped_df = df_top_towns.groupby(['town', 'flat_type'])['price_per_sqm'].median().unstack()
        # Make the figure larger
        # Plot the data
        grouped_df.plot(kind='bar', ax=ax, width=0.8)  # Adjust width as necessary
        # Set chart title and labels
        ax.set_xlabel('Town')
        ax.set_ylabel('Median Price per Sq Meter (SGD)')
        # Rotate the x-tick labels for better readability
        plt.setp(ax.get_xticklabels(), rotation=45, horizontalalignment='right')
        # Legend configuration
        ax.legend(title='Flat Type', loc='upper left', bbox_to_anchor=(1, 1), fontsize='small')
        # Save figure
        save_plot_as_image(plt, 'price_distribution_by_town_and_flat_type')
        plt.close()

    def plot_lease_commencement_date(df):
        _, ax = plt.subplots() 
        lease_commence_analysis = df.groupby(df['lease_commence_date'].dt.year)['price_per_sqm'].median()
        lease_commence_analysis.plot(kind='line')
        # Set chart title and labels
        ax.set_xlabel('Lease Commencement Date')
        ax.set_ylabel('Median Price per Sq Meter (SGD)')
        save_plot_as_image(plt, 'lease_commencement_date')
        plt.close()

    def plot_remaining_lease(df):
        _, ax = plt.subplots() 
        remaining_lease_analysis = df.groupby('remaining_lease')['price_per_sqm'].median()
        remaining_lease_analysis.plot(kind='line')
        # Set chart title and labels
        ax.set_xlabel('Remaining Lease in Years')
        ax.set_ylabel('Median Price per Sq Meter (SGD)')
        save_plot_as_image(plt, 'remaining_lease')
        plt.close()

    def plot_distance_to_cbd_distribution(df: pd.DataFrame):
        _, ax = plt.subplots()
        # Plot a histogram of the distances.
        df['distance_from_cbd'].hist(bins=50, ax=ax, edgecolor='black')
        ax.set_xlabel('Distance from CBD (km)')
        ax.set_ylabel('Number of Properties')
        save_plot_as_image(plt, 'dist_to_cbd_distribution')
        plt.close()

    def plot_price_vs_distance_to_cbd(df: pd.DataFrame):
        _, ax = plt.subplots()
        # Scatter plot
        sns.scatterplot(x='distance_from_cbd', y='price_per_sqm', data=df, alpha=0.6)
        # Regression line
        sns.regplot(x='distance_from_cbd', y='price_per_sqm', data=df, scatter=False, color='red')
        ax.set_xlabel('Distance from CBD (km)')
        ax.set_ylabel('Price Per Sqm (SGD)')
        save_plot_as_image(plt, 'dist_to_cbd')
        plt.close()

    plot_real_prices(df)
    plot_floor_area_distribution(df)
    plot_price_distribution_by_town(df)
    plot_avg_price_per_sqm_by_town_flat_type(df)
    plot_lease_commencement_date(df)
    plot_remaining_lease(df)
    plot_distance_to_cbd_distribution(df)
    plot_price_vs_distance_to_cbd(df)

def plot_mrt_info(df):
    def plot_proximity_to_mrts(df):
        _, ax = plt.subplots() 
        grouped_data = df.groupby('num_mrts_within_radius')['price_per_sqm'].median()
        grouped_data.plot(kind='line')
        ax.set_xlabel(f'Number of MRT Stations within {PROXIMITY_RADIUS}km')
        ax.set_ylabel('Median Price Per Sqm (SGD)')
        plt.suptitle('')  # Suppress the automatic title
        save_plot_as_image(plt, 'num_mrts_within_radius')
        plt.close()

    def plot_distance_to_mrt(df):
        _, ax = plt.subplots() 
        # Aggregated scatter plot to reduce noise
        df_filtered = df.dropna(subset=['nearest_mrt'])
        bins = pd.cut(df_filtered['dist_to_nearest_mrt'], bins=np.arange(0, df_filtered['dist_to_nearest_mrt'].max() + 0.1, 0.1))
        grouped = df_filtered.groupby(bins)['price_per_sqm'].median().reset_index()
        # Get the mid-point of each interval for plotting
        grouped['dist_mid'] = grouped['dist_to_nearest_mrt'].apply(lambda x: x.mid)
        # Scatter plot
        sns.scatterplot(x='dist_mid', y='price_per_sqm', data=grouped, alpha=0.6)
        # Regression line
        sns.regplot(x='dist_mid', y='price_per_sqm', data=grouped, scatter=False, color='red')        
        ax.set_xlabel('Distance to Nearest MRT (km)')
        ax.set_ylabel('Median Price Per Sqm (SGD)')
        save_plot_as_image(plt, 'dist_to_nearest_mrt')
        plt.close()
    
    def plot_different_mrts(df):
        _, ax = plt.subplots() 
        df_filtered = df[df['dist_to_nearest_mrt'].notnull() & (df['dist_to_nearest_mrt'] < 2)]
        # Group by 'nearest_mrt' and calculate median 'price_per_sqm', then sort by values
        average_prices_by_mrt = df_filtered.groupby('nearest_mrt')['price_per_sqm'].median().sort_values(ascending=False)
        # Sort values and select the top n and bottom n
        n = 8
        top_mrts = average_prices_by_mrt.nlargest(n)
        bottom_mrts = average_prices_by_mrt.nsmallest(n)
        combined_mrts = pd.concat([top_mrts, bottom_mrts]).sort_values()
        # Plot
        combined_mrts.plot(kind='bar')
        ax.set_xlabel('Nearest MRT Station')
        ax.set_ylabel('Median Price Per Sqm (SGD)')
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        save_plot_as_image(plt, 'different_mrt_prices')
        plt.close()

    plot_proximity_to_mrts(df)
    plot_distance_to_mrt(df)
    plot_different_mrts(df)

def plot_pri_sch_info(df):
    def plot_num_nearest_pri_sch_info(df):
        _, ax = plt.subplots() 
        df['num_pri_sch_within_radius'] = pd.to_numeric(df['num_pri_sch_within_radius'], errors='coerce')
        df['price_per_sqm'] = pd.to_numeric(df['price_per_sqm'], errors='coerce')
        # Aggregate the data
        agg_data = df.groupby('num_pri_sch_within_radius')['price_per_sqm'].median().reset_index()
        # Scatter plot
        sns.scatterplot(x='num_pri_sch_within_radius', y='price_per_sqm', data=agg_data, alpha=0.6)
        # Regression line
        ax.set_xlabel('Number of Nearby Primary Schools')
        ax.set_ylabel('Price Per Sqm (SGD)')
        save_plot_as_image(plt, 'num_nearest_pri_sch')
        plt.close()

    def plot_nearest_pri_sch_info(df):
        _, ax = plt.subplots() 
        n = 8
        df['price_per_sqm'] = pd.to_numeric(df['price_per_sqm'], errors='coerce') 
        # Calculate the median or median price per sqm for each school
        school_price_stats = df.groupby('school_name')['price_per_sqm'].median().sort_values()
        # Take the top N and bottom N schools
        top_schools = school_price_stats.nlargest(n)
        bottom_schools = school_price_stats.nsmallest(n)
        # Combine top and bottom schools into one Series
        selected_schools = pd.concat([top_schools, bottom_schools]).index
        # Filter the original dataframe for only the selected schools
        selected_schools_df = df[df['school_name'].isin(selected_schools)]
        # Create the box plot
        plt.figure(figsize=(15, 10))
        sns.boxplot(x='school_name', y='price_per_sqm', data=selected_schools_df, order=selected_schools)
        # Improve the aesthetics
        plt.xticks(rotation=45)  # Rotate the labels for better readability
        ax.set_xlabel('Nearest School')
        ax.set_ylabel('Price Per Sqm (SGD)')
        save_plot_as_image(plt, 'nearest_pri_sch')
        plt.close()

    plot_num_nearest_pri_sch_info(df)
    plot_nearest_pri_sch_info(df)