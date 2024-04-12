import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

from reporting.constants import IMAGE_PATHS, PDF_PATH, TOP_OF_PAGE_Y, LOWEST_POSITION_Y, CHART_HEIGHT, CHART_WIDTH, CHART_GAP, TITLE_GAP

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


def plot_real_prices(df: pd.DataFrame):
    image_path = IMAGE_PATHS['real_prices']['path']
    _, ax = plt.subplots(figsize=(14, 4.5))
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
    # Save the plot
    plt.title(IMAGE_PATHS['real_prices']['title'])
    plt.tight_layout()
    plt.savefig(image_path)
    plt.close()

def plot_floor_area_distribution(df: pd.DataFrame):
    image_path = IMAGE_PATHS['floor_area_distribution']['path']
    plt.hist(df['floor_area_sqm'], bins=50, edgecolor='black')
    plt.title(IMAGE_PATHS['floor_area_distribution']['title'])
    plt.tight_layout()
    plt.savefig(image_path)
    plt.close()

def plot_price_distribution_by_town(df):
    image_path = IMAGE_PATHS['price_distribution_by_town']['path']
    df['price_per_sqm'] = df['resale_price'] / df['floor_area_sqm']
    # Select top N towns for clarity
    top_towns = df['town'].value_counts().nlargest(10).index
    df_top_towns = df[df['town'].isin(top_towns)]
    # Map towns to numeric positions
    town_positions = {town: pos for pos, town in enumerate(sorted(top_towns))}
    df_top_towns['town_position'] = df_top_towns['town'].map(town_positions)
    # Create boxplot
    _, ax = plt.subplots(figsize=(14, 8))
    df_top_towns.boxplot(column='price_per_sqm', by='town_position', ax=ax)
    # Set labels
    ax.set_xticklabels([town for town in sorted(top_towns)], rotation=45, ha='right')
    ax.set_xlabel('Town')
    ax.set_ylabel('Price per Square Meter (SGD)')
    # Save figure
    plt.suptitle('')  # Suppress the automatic title
    plt.title(IMAGE_PATHS['price_distribution_by_town']['title'])
    plt.tight_layout()
    plt.savefig(image_path)
    plt.close()

def plot_avg_price_per_sqm_by_town_flat_type(df):
    image_path = IMAGE_PATHS['price_distribution_by_town_and_flat_type']['path']
     # Calculate price per square meter
    df['price_per_sqm'] = df['resale_price'] / df['floor_area_sqm']
    # Group by town and flat type, then calculate the average price per square meter
    grouped_df = df.groupby(['town', 'flat_type'])['price_per_sqm'].mean().unstack()
    # Make the figure larger
    _, ax = plt.subplots(figsize=(20, 10))  # Increase the figure size
    # Plot the data
    grouped_df.plot(kind='bar', ax=ax, width=0.8)  # Adjust width as necessary
    # Set chart title and labels
    ax.set_title('Average Price Per Square Meter by Town and Flat Type', fontsize=16)
    ax.set_xlabel('Town', fontsize=14)
    ax.set_ylabel('Average Price per Sq Meter (SGD)', fontsize=14)
    # Rotate the x-tick labels for better readability
    plt.setp(ax.get_xticklabels(), rotation=45, horizontalalignment='right')
    # Legend configuration
    ax.yaxis.grid(True, linestyle='--', which='major', color='grey', alpha=.25)
    ax.legend(title='Flat Type', fontsize=12, title_fontsize='13')
    # Save figure
    plt.title(IMAGE_PATHS['price_distribution_by_town_and_flat_type']['title'])
    plt.tight_layout()
    plt.savefig(image_path)
    plt.close()
