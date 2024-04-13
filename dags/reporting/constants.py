IMAGE_PATHS = {
    'real_prices': {
        'title': 'Real vs Nominal Resale Prices',
        'path': './real_prices.png'
    },
    'floor_area_distribution': {
        'title': 'Price by Floor Area',
        'path': './floor_area_distribution.png'
    },
    'price_distribution_by_town': {
        'title': 'Price per Square Meter Distribution by Town',
        'path': './price_distribution_by_town.png'
    },
    'price_distribution_by_town_and_flat_type': {
        'title': 'Average Price Per Square Meter by Town and Flat Type',
        'path': './price_distribution_by_town_and_flat_type.png'
    },
    'lease_commencement_date': {
        'title': 'Average Resale Price Per Sqm Over Lease Commence Date',
        'path': './lease_commencement_date.png'
    },
    'remaining_lease': {
        'title': 'Average Resale Price Per Sqm Over Remaining Lease',
        'path': './remaining_lease.png'
    },
    'num_mrts_within_radius': {
        'title': 'Average Price Per Sqm by Number of Nearby MRT Stations',
        'path': './num_mrts_within_radius.png'
    },
    'dist_to_nearest_mrt': {
        'title': 'Price Per Sqm vs. Distance to Nearest MRT',
        'path': './dist_to_nearest_mrt.png'
    },
    'different_mrt_prices': {
        'title': 'Top and Bottom MRT Stations by Average Price Per Sqm (within 2km)',
        'path': './different_mrt_prices.png'
    }
}
PDF_PATH = './report.pdf'
TOP_OF_PAGE_Y = 200
LOWEST_POSITION_Y = 100
CHART_HEIGHT = 450
CHART_WIDTH = 450
CHART_GAP = 100
TITLE_GAP = 10

HTML_START = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HDB Resale Price Analysis Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 0;
        }}
        .container {{
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
        }}
        .header h1 {{
            margin: 0;
        }}
        .header .date {{
            font-size: 16px;
            color: #555;
        }}
        .graphs-container {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            grid-gap: 20px;
        }}
        .graph-item {{
            display: flex;
            flex-direction: column;
            align-items: center;
        }}
        .graph {{
            background-color: #f4f4f4;
            padding: 20px;
            border-radius: 5px;
            height: 400px;
            display: flex;
            justify-content: center;
            align-items: center;
            font-size: 18px;
            color: #666;
        }}
        .graph-caption {{
            margin-top: 10px;
            font-size: 14px;
            color: #555;
        }}
        .graph img {{
            max-width: 100%;
            max-height: 100%;
            object-fit: contain;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>HDB Resale Price Analysis Report</h1>
            <div class="date">{date}</div>
        </div>
    
<div class="graphs-container">
'''

PLOT_TEMPLATE = '''
<div class="graph-item">
    <div class="graph">{image}</div>
    <div class="graph-caption">{caption}</div>
</div>
'''

HTML_END = '''
        </div>
    </div>
</body>
</html>
'''