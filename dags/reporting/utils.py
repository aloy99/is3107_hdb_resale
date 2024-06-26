import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from datetime import datetime
import gc

import base64

from reporting.constants import IMAGE_PATHS, METRIC_PATHS, HTML_PATH, HTML_START, HTML_END, PLOT_TEMPLATE, METRIC_TEMPLATE
from common.constants import FETCHING_RADIUS, PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS

def save_plot_as_image(plt, plot_name):
    plt.title(IMAGE_PATHS[plot_name]['title'])
    plt.tight_layout()
    plt.savefig(IMAGE_PATHS[plot_name]['path'])

def save_table_as_html(df, metric_name):
    with open(METRIC_PATHS[metric_name]['path'], 'w') as f:
        f.write(df.to_html())

def create_html_report():
    report_body = HTML_START.format(date = datetime.today().strftime('%Y-%m-%d')).strip()
    for item in IMAGE_PATHS.values():
        data_uri = base64.b64encode(open(item['path'], 'rb').read()).decode('utf-8')
        img_tag = '<img src="data:image/png;base64,{0}">'.format(data_uri)
        curr_row = PLOT_TEMPLATE.format(image = img_tag, caption = item['title'])
        report_body += curr_row
    for metric in METRIC_PATHS.values():
        metrics_table = open(metric['path'], 'r').read()
        curr_row = METRIC_TEMPLATE.format(table = metrics_table)
        report_body += curr_row
    report_body += HTML_END.strip()
    with open(HTML_PATH, 'w') as f:
        f.write(report_body.replace('\n',''))
    return report_body.replace('\n','')

def plot_default_features(df):

    def plot_real_prices(df: pd.DataFrame):
        _, ax = plt.subplots() 
        # Plot Unadjusted Prices
        df.groupby('transaction_month')['resale_price'].mean().plot(ax=ax, color='#18bddd', label='Unadjusted for Inflation')
        # Plot Adjusted Prices
        df.groupby('transaction_month')['real_resale_price'].mean().plot(ax=ax, color='#df9266', label='Adjusted for Inflation')
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
        grouped_df = df_top_towns.groupby(['town', 'flat_type'])['price_per_sqm'].mean().unstack()
        # Make the figure larger
        # Plot the data
        grouped_df.plot(kind='bar', ax=ax, width=0.8)  # Adjust width as necessary
        # Set chart title and labels
        ax.set_xlabel('Town')
        ax.set_ylabel('Average Price per Sq Meter (SGD)')
        # Rotate the x-tick labels for better readability
        plt.setp(ax.get_xticklabels(), rotation=45, horizontalalignment='right')
        # Legend configuration
        ax.legend(title='Flat Type', loc='upper left', bbox_to_anchor=(1, 1), fontsize='small')
        # Save figure
        save_plot_as_image(plt, 'price_distribution_by_town_and_flat_type')
        plt.close()

    def plot_lease_commencement_date(df):
        _, ax = plt.subplots() 
        lease_commence_analysis = df.groupby(df['lease_commence_date'].dt.year)['price_per_sqm'].mean()
        lease_commence_analysis.plot(kind='line')
        # Set chart title and labels
        ax.set_xlabel('Lease Commencement Date')
        ax.set_ylabel('Average Price per Sq Meter (SGD)')
        save_plot_as_image(plt, 'lease_commencement_date')
        plt.close()

    def plot_remaining_lease(df):
        _, ax = plt.subplots() 
        remaining_lease_analysis = df.groupby('remaining_lease')['price_per_sqm'].mean()
        remaining_lease_analysis.plot(kind='line')
        # Set chart title and labels
        ax.set_xlabel('Remaining Lease in Years')
        ax.set_ylabel('Average Price per Sq Meter (SGD)')
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
        df_new = df.copy()
        df_new['num_mrts_within_radius'] = df_new.apply(lambda row: 0 if row['distance_to_mrt'] is None or row['distance_to_mrt'] >= PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS else 1, axis=1)
        df_grouped = df_new.groupby('flat_id').agg(
            num_mrts_within_radius=('num_mrts_within_radius', 'sum'),
            price_per_sqm=('price_per_sqm', 'mean')
        ).reset_index()
        # Use seaborn's boxplot to plot this data
        plt.figure(figsize=(12, 6))
        sns.boxplot(x='num_mrts_within_radius', y='price_per_sqm', data=df_grouped)
        ax.set_xlabel(f'Number of MRT Stations within {PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS}km')
        ax.set_xlabel('Average Price Per Sqm (SGD)')
        plt.xticks(rotation=45, ha='right')  # Rotate x-axis labels if they overlap
        # Save the plot as an image
        save_plot_as_image(plt, 'num_mrts_within_radius')
        plt.close()

    def plot_distance_to_nearest_mrt(df):
        _, ax = plt.subplots()
        df = df[~df['distance_to_mrt'].isna()]
        nearest_mrts = df.groupby('flat_id').agg(
            distance_to_mrt=('distance_to_mrt', 'min'),  # Minimum distance to MRT
            price_per_sqm=('price_per_sqm', 'mean')  # Average price per sqm
        ).reset_index()
        # Use quantile-based binning or user-defined intervals
        bin_edges = np.quantile(nearest_mrts['distance_to_mrt'], np.linspace(0, 1, num=10))
        bins = pd.cut(nearest_mrts['distance_to_mrt'], bins=bin_edges, include_lowest=True)
        grouped = nearest_mrts.groupby(bins)['price_per_sqm'].mean().reset_index()
        grouped['dist_mid'] = grouped['distance_to_mrt'].apply(lambda x: x.mid)
        sns.scatterplot(x='dist_mid', y='price_per_sqm', data=grouped, alpha=0.6)
        sns.regplot(x='dist_mid', y='price_per_sqm', data=grouped, scatter=False, color='red')
        ax.set_xlabel('Distance to Nearest MRT (km)')
        ax.set_ylabel('Average Price Per Sqm (SGD)')
        save_plot_as_image(plt, 'dist_to_nearest_mrt')
        plt.show()

    def plot_different_mrts(df):
        _, ax = plt.subplots() 
        df_filtered = df.groupby('flat_id').agg(
            distance_to_mrt=('distance_to_mrt', 'min'),  # Minimum distance to MRT
            price_per_sqm=('price_per_sqm', 'mean') 
        ).reset_index()
        df_filtered = df[df['distance_to_mrt'] < PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS]
        # Group by 'nearest_mrt' and calculate mean 'price_per_sqm', then sort by values
        average_prices_by_mrt = df_filtered.groupby('mrt')['price_per_sqm'].mean().sort_values(ascending=False)
        # Sort values and select the top n and bottom n
        n = 8
        top_mrts = average_prices_by_mrt.nlargest(n)
        bottom_mrts = average_prices_by_mrt.nsmallest(n)
        combined_mrts = pd.concat([top_mrts, bottom_mrts]).sort_values()
        # Plot
        combined_mrts.plot(kind='bar')
        ax.set_xlabel('Nearest MRT Station')
        ax.set_ylabel('Average Price Per Sqm (SGD)')
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
        save_plot_as_image(plt, 'different_mrt_prices')
        plt.close()

    plot_proximity_to_mrts(df)
    plot_distance_to_nearest_mrt(df)
    plot_different_mrts(df)

def plot_pri_sch_info(df):
    def plot_price_vs_schools(df):
        df_new = df.copy()
        df_new['num_pri_sch_within_radius'] = df_new['distance_to_school'].transform(lambda x: 0 if x is None or x >= PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS else 1)
        df_grouped = df_new.groupby('flat_id').agg(
            num_pri_sch_within_radius=('num_pri_sch_within_radius', 'sum'),
            price_per_sqm=('price_per_sqm', 'mean')
        ).reset_index()
        _, ax = plt.subplots()
        # Boxplot
        sns.boxplot(x='num_pri_sch_within_radius', y='price_per_sqm', data=df_grouped)
        ax.set_xlabel(f'Number of Primary Schools within {PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS} Radius')
        ax.set_ylabel('Price per sqm (SGD)')
        save_plot_as_image(plt, 'num_pri_sch_within_radius_boxplot')
        plt.close()
        del df_new
        del df_grouped

    def plot_nearest_pri_schs(df):
        df = df[~df['distance_to_school'].isna()]
        _, ax = plt.subplots()
        # Scatter plot
        sns.scatterplot(x='distance_to_school', y='price_per_sqm', data=df, alpha=0.5, edgecolor=None)
        # To avoid overplotting in scatter plots, reduce alpha and remove edgecolor
        # Regression line
        sns.regplot(x='distance_to_school', y='price_per_sqm', data=df, scatter=False, color='red')
        ax.set_xlabel('Distance to Nearest Primary School (km)')
        ax.set_ylabel('Price per sqm (SGD)')
        save_plot_as_image(plt, 'dist_to_nearest_pri_sch')
        plt.close()
        del df

    def plot_price_vs_school_type(df):
        df_filtered = df[df['distance_to_school'] < PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS]
        _, ax = plt.subplots()
        sns.boxplot(x='type_code', y='price_per_sqm', data=df_filtered)
        ax.set_xlabel('School Type Code')
        ax.set_ylabel('Price per sqm (SGD)')
        save_plot_as_image(plt, 'resale_price_vs_school_type')
        plt.close()
        del df_filtered

    def plot_price_vs_school_nature(df):
        df_filtered = df[df['distance_to_school'] < PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS]
        _, axes = plt.subplots()
        sns.boxplot(x='nature_code', y='price_per_sqm', data=df_filtered)
        axes.set_xlabel('School Nature Code')
        axes.set_ylabel('Price per sqm (SGD)')
        save_plot_as_image(plt, 'resale_price_vs_school_nature')
        plt.close()
        del df_filtered

    def plot_price_vs_special_programs(df):
        df_filtered = df[df['distance_to_school'] < PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS]
        _, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=True)
        sns.boxplot(x='sap_ind', y='price_per_sqm', data=df, ax=axes[0])
        axes[0].set_title('SAP Schools')
        axes[0].set_xlabel('SAP Indicator')
        axes[0].set_ylabel('Price per sqm (SGD)')
        sns.boxplot(x='autonomous_ind', y='price_per_sqm', data=df_filtered, ax=axes[1])
        axes[1].set_title('Autonomous Schools')
        axes[1].set_xlabel('Autonomous Indicator')
        axes[1].set_ylabel('')
        sns.boxplot(x='gifted_ind', y='price_per_sqm', data=df, ax=axes[2])
        axes[2].set_title('Gifted Education Programme')
        axes[2].set_xlabel('Gifted Indicator')
        axes[2].set_ylabel('')
        save_plot_as_image(plt, 'resale_price_vs_special_programs')
        plt.close()
        del df_filtered
        
    # plot_price_vs_schools(df)
    plot_nearest_pri_schs(df)
    # plot_price_vs_school_type(df)
    # plot_price_vs_school_nature(df)
    # plot_price_vs_special_programs(df)

def plot_park_info(df):
    # Average price per sqm for flats by number of nearby parks
    def average_price_per_sqm_by_num_parks(df):
        df_new = df.copy()
        df_new['num_parks_within_radius'] = df_new.apply(lambda row: 0 if row['distance_to_park'] is None or row['distance_to_park'] >= PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS else 1, axis=1)
        df_grouped = df_new.groupby('flat_id').agg(
            num_parks_within_radius=('num_parks_within_radius', 'sum'),
            price_per_sqm=('price_per_sqm', 'mean')
        ).reset_index()
        _, ax = plt.subplots()
        plt.figure(figsize=(10, 6))
        sns.boxplot(x='num_parks_within_radius', y='price_per_sqm', data=df_grouped)
        ax.set_xlabel('Number of Parks within Radius')
        ax.set_ylabel('Average Price Per Sqm (SGD)')
        save_plot_as_image(plt, 'num_parks_within_radius')
        plt.close()
    
    def prices_near_specific_parks(df):
        n = 8
        _, ax = plt.subplots()
        df_near_parks = df[df['distance_to_park'] <= PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS]    
        # Group by park name and calculate average price per sqm
        park_price_sqm = df_near_parks.groupby('park').agg(
            average_price_per_sqm=('price_per_sqm', 'mean')
        ).reset_index().sort_values(by='average_price_per_sqm', ascending=True)
        # Combine the top n and bottom n parks
        combined_parks = pd.concat([park_price_sqm.head(n), park_price_sqm.tail(n)])
        # Create a single plot
        plt.figure(figsize=(12, 8))  # Adjust figure size as needed
        sns.barplot(x='park', y='average_price_per_sqm', data=combined_parks, order=combined_parks['park'])
        ax.set_xlabel('Park')
        ax.set_ylabel('Average Price Per Sqm (SGD)')
        plt.xticks(rotation=45, ha='right')
        save_plot_as_image(plt, 'prices_near_specific_parks')
        plt.close()

    average_price_per_sqm_by_num_parks(df)
    prices_near_specific_parks(df)

def plot_supermarket_info(df):
    
    def plot_distance_to_nearest_supermarket(df):
        _, ax = plt.subplots()
        df = df[~df['distance_to_supermarket'].isna()]
        nearest_supermarkets = df.groupby('flat_id').agg(
            distance_to_supermarket=('distance_to_supermarket', 'min'),  # Minimum distance to supermarket
            price_per_sqm=('price_per_sqm', 'mean')  # Average price per sqm
        ).reset_index()
        # Use quantile-based binning or user-defined intervals
        bin_edges = np.quantile(nearest_supermarkets['distance_to_supermarket'], np.linspace(0, 1, num=10))
        bins = pd.cut(nearest_supermarkets['distance_to_supermarket'], bins=bin_edges, include_lowest=True)
        grouped = nearest_supermarkets.groupby(bins)['price_per_sqm'].mean().reset_index()
        grouped['dist_mid'] = grouped['distance_to_supermarket'].apply(lambda x: x.mid)
        sns.scatterplot(x='dist_mid', y='price_per_sqm', data=grouped, alpha=0.6)
        sns.regplot(x='dist_mid', y='price_per_sqm', data=grouped, scatter=False, color='red')
        ax.set_xlabel('Distance to Nearest supermarket (km)')
        ax.set_ylabel('Average Price Per Sqm (SGD)')
        save_plot_as_image(plt, 'dist_to_nearest_supermarket')
        plt.show()

    plot_distance_to_nearest_supermarket(df)


