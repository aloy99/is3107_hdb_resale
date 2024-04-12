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
