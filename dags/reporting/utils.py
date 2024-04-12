import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

from reporting.constants import IMAGE_PATHS, PDF_PATH, TOP_OF_PAGE_Y, LOWEST_POSITION_Y, CHART_HEIGHT, CHART_WIDTH, CHART_GAP, TITLE_GAP

def save_plot_as_image(plt, plot_name):
    plt.title(IMAGE_PATHS[plot_name]['title'])
    plt.tight_layout()
    plt.savefig( IMAGE_PATHS[plot_name]['path'])

def add_image_to_pdf(canvas, image_path, y_position, title=None):
    if title:
        canvas.drawString(CHART_GAP, y_position + TITLE_GAP, title)
    canvas.drawImage(image_path, CHART_GAP, y_position, width=CHART_WIDTH, height=CHART_HEIGHT)

def consolidate_report():
    c = canvas.Canvas(PDF_PATH, pagesize=letter)
    y_position = TOP_OF_PAGE_Y  # Start from top of page
    for item in IMAGE_PATHS:
        # Check if we need new page
        if y_position < LOWEST_POSITION_Y:
            c.showPage()
            y_position = TOP_OF_PAGE_Y  # Reset position
        add_image_to_pdf(c, IMAGE_PATHS[item]['path'], y_position, title= IMAGE_PATHS[item]['title'])
        y_position -= CHART_HEIGHT + CHART_GAP  # Move down the position for the next image. Adjust as needed.
    c.save()
    print(f"PDF report saved to {PDF_PATH}")

def plot_default_features(df):

    def plot_real_prices(df: pd.DataFrame):
        _, ax = plt.subplots()
        # Plot Unadjusted Prices
        df.groupby('transaction_month')['resale_price'].median().plot(ax=ax, color='#18bddd', label='Unadjusted for Inflation')
        # Plot Adjusted Prices
        df.groupby('transaction_month')['real_resale_price'].median().plot(ax=ax, color='#df9266', label='Adjusted for Inflation')
        # Get limits of data.
        x_min, x_max = ax.get_xlim()
        y_min, y_max = ax.get_ylim()
        # Apply the new limits with padding.
        padding_factor = 0.1 
        x_padding = (x_max - x_min) * padding_factor
        y_padding = (y_max - y_min) * padding_factor
        ax.set_xlim(x_min - x_padding, x_max + x_padding)
        ax.set_ylim(y_min - y_padding, y_max + y_padding)
        ax.set_xlabel('Date')
        ax.set_ylabel('Price in SGD ($)')
        ax.legend()
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
    
    def plot_proximity_to_mrts(df):
        _, ax = plt.subplots() 
        grouped_data = df.groupby('num_mrts_within_3km')['resale_price'].mean()
        grouped_data.plot(kind='line')
        ax.set_xlabel('Number of MRT Stations within 2 km')
        ax.set_ylabel('Average Resale Price (SGD)')
        save_plot_as_image(plt, 'num_mrts_within_3km')
        plt.close()
        plot_real_prices(df)
        plot_floor_area_distribution(df)
        plot_price_distribution_by_town(df)
        plot_avg_price_per_sqm_by_town_flat_type(df)
        plot_lease_commencement_date(df)
        plot_remaining_lease(df)
        plot_proximity_to_mrts(df)

def plot_mrt_info(df):
    def plot_distance_to_mrt(df):
        _, ax = plt.subplots() 
        # Aggregated scatter plot with mean resale price for binned distance ranges
        bins = pd.cut(df['dist_to_nearest_mrt'], bins=np.arange(0, df['dist_to_nearest_mrt'].max() + 0.1, 0.1))
        grouped = df.groupby(bins)['resale_price'].mean().reset_index()
        # Get the mid-point of each interval for plotting
        grouped['dist_mid'] = grouped['dist_to_nearest_mrt'].apply(lambda x: x.mid)
        plt.scatter(grouped['dist_mid'], grouped['resale_price'], alpha=0.6)
        ax.set_xlabel('Distance to Nearest MRT (km)')
        ax.set_ylabel('Average Resale Price (SGD)')
        save_plot_as_image(plt, 'dist_to_nearest_mrt')
        plt.close()
    
    def plot_different_mrts(df):
        _, ax = plt.subplots() 
        average_prices_by_mrt = df.groupby('nearest_mrt')['resale_price'].mean().sort_values(ascending=False)
        plt.figure(figsize=(15, 7))
        average_prices_by_mrt.plot(kind='bar')
        ax.set_xlabel('Nearest MRT Station')
        ax.set_ylabel('Average Resale Price (SGD)')
        plt.xticks(rotation=90)
        save_plot_as_image(plt, 'different_mrt_prices')
        plt.show()

    plot_distance_to_mrt(df)
    plot_different_mrts(df)
