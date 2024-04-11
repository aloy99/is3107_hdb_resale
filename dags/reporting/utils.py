import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reporting.constants import IMAGE_PATHS

def add_image_to_pdf(image_path, pdf_path, title=None):
    c = canvas.Canvas(pdf_path, pagesize=letter)
    if title:
        c.drawString(100, 750, title)
    c.drawImage(image_path, 100, 500, width=400, height=300)  # Adjust size as needed
    c.showPage()
    c.save()

def plot_real_prices(df: pd.DataFrame):
    image_path = IMAGE_PATHS['cpi']
    fig, ax = plt.subplots(figsize=(14, 4.5))
    fig.suptitle('Median HDB Resale Prices Over the Years', fontsize=18)

    # Plot Unadjusted Prices
    df.groupby('transaction_month')['resale_price'].median().plot(ax=ax, color='#18bddd', label='Unadjusted for Inflation')
    
    # Plot Adjusted Prices
    df.groupby('transaction_month')['real_resale_price'].median().plot(ax=ax, color='#df9266', label='Adjusted for Inflation')
    
    # Settings for the combined chart
    ax.set_xlabel('Date')
    ax.set_ylabel('Price in SGD ($)')
    ax.set_title('HDB Resale Prices: Adjusted vs Unadjusted for Inflation')
    ax.set_ylim(0, 500000)
    ax.legend()
    
    # Save the plot
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(image_path)
    plt.close()