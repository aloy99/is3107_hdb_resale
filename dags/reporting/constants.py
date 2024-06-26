from common.constants import PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS

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
        'title': f'Top and Bottom MRT Stations by Average Price Per Sqm (within {PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS}km)',
        'path': './different_mrt_prices.png'
    },
    'dist_to_cbd_distribution': {
        'title': 'Distribution of flats from CBD',
        'path': './dist_to_cbd_distribution.png'
    },
    'dist_to_cbd': {
        'title': 'Price per sqm vs. Distance from CBD',
        'path': './dist_to_cbd.png'
    },
    'num_pri_sch_within_radius_boxplot': {
         'title': f'Box Plot of Resale Prices by Number of Primary Schools within {PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS}km',
         'path': './num_pri_sch_within_radius_boxplot.png'
     },
    'dist_to_nearest_pri_sch': {
         'title': 'Price per sqm vs. Distance to Nearest Primary School',
         'path': './dist_to_nearest_pri_sch.png'
     },
    'resale_price_vs_school_type': {
         'title': 'Price per sqm by School Type',
         'path': './resale_price_vs_school_type.png'
     },
    'resale_price_vs_school_nature': {
         'title': 'Price per sqm by School Nature',
         'path': './resale_price_vs_school_nature.png'
     },
    'resale_price_vs_special_programs': {
         'title': 'Price per sqm by Special Programs',
         'path': './resale_price_vs_special_programs.png'
     },
    'num_parks_within_radius': {
         'title': f'Average Price Per Sqm by Number of Parks within {PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS} meters',
         'path': './num_parks_within_radius.png'
     },
    'prices_near_specific_parks': {
         'title': f'Top and Bottom Parks by Average Price Per Sqm within {PROXIMITY_RADIUS_FOR_FILTERED_ANALYSIS}km',
         'path': './prices_near_specific_parks.png'
     },
     'dist_to_nearest_supermarket': {
         'title': 'Price per sqm vs. Distance to Nearest Supermarket',
         'path': './dist_to_nearest_supermarket.png'
     },
     'linear_regression_scatter_plot': {
         'title': 'Observed vs Predicted Prices for Linear Regression',
         'path': './linear_regression_scatter_plot.png'
     },
     'linear_regression_feature_importance': {
         'title': 'Feature Importance for Linear Regression',
         'path': './linear_regression_feature_importance.png'
     },
    'random_forest_scatter_plot': {
         'title': 'Observed vs Predicted Prices for Random Forest',
         'path': './random_forest_scatter_plot.png'
     },
     'random_forest_feature_importance': {
         'title': 'Feature Importance for Random Forest',
         'path': './random_forest_feature_importance.png'
     },
}

METRIC_PATHS = {
    'lr_metrics': {
        'path': './lr_metrics.html'
    },
    'rf_metrics': {
        'path': './rf_metrics.html'
    }
}

HTML_PATH = './report.html'
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

METRIC_TEMPLATE = '''
<div class="graph-item">
    {table}
</div>
'''

HTML_END = '''
        </div>
    </div>
</body>
</html>
'''