import os
import json
import zipfile

from airflow.decorators import dag, task
from datetime import datetime, timedelta

import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
import matplotlib.pyplot as plt
import matplotlib

os.environ['KAGGLE_CONFIG_DIR'] = '/.kaggle'
USER_NAME = "sujaykapadnis"
DATASET_NAME = "lets-do-some-coffee-tasting"

STRENGTH_COL = 'How strong do you like your coffee?'
FAV_DRINK_COL = 'What is your favorite coffee drink?'
DATASET_COLS = [
    'Education Level',
    'Employment Status',
    'In total, much money do you typically spend on coffee in a month?',
    STRENGTH_COL,
    FAV_DRINK_COL
]
PIE_CHART_COLS = set(
    [
        'Education Level',
        STRENGTH_COL
    ] 
)
BAR_CHART_COLS = set(
    [
        'Employment Status',
        'In total, much money do you typically spend on coffee in a month?',
        FAV_DRINK_COL
    ] 
)


STRENGTH_MAP = {
    'Somewhat strong': 'Strong',
    'Very strong': 'Strong',
    'Somewhat light': 'Weak',
    'Medium': 'Medium',
    'Weak': 'Weak'
}

JSON_PATH = 'transformed_coffee_data.json'

PDF_PATH = 'coffee_report.pdf'

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10)
}

matplotlib.use("agg")


def pie_chart(data, image_path, title):
    categories = list(data.keys())
    quantities = list(data.values())
    
    fig, ax = plt.subplots(figsize=(8, 8))
    ax.pie(quantities, labels=categories, autopct=lambda pct: '{:1.1f}%'.format(pct) if pct > 5 else '', startangle=90, labeldistance=None)
    ax.set_title(title)
    handles, labels = ax.axes.get_legend_handles_labels()
    ax.legend(handles, labels, prop={'size':8},
          bbox_to_anchor=(0.3,0.3),
          bbox_transform=fig.transFigure)
    
    plt.savefig(image_path)
    
    plt.close()

    return image_path

def bar_chart(data, image_path, title):
    categories = list(data.keys())
    quantities = list(data.values())
    
    fig, ax = plt.subplots(figsize=(10,10))
    ax.bar(categories, quantities, width=0.5)
    ax.set_xlabel('Categories')
    ax.set_ylabel('Quantities')
    ax.set_title(title)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    
    plt.savefig(image_path, bbox_inches="tight")
    
    plt.close()

    return image_path


@dag(dag_id='extract_survey_data', default_args=default_args, schedule=None, catchup=False, tags=['assignment3'])
def extract_survey_data():
    @task
    def extract_coffee_data():
        os.environ['KAGGLE_CONFIG_DIR'] = '/.kaggle'
        import kaggle #import has to be placed here, or else authentication fails. import alone alr tries to authenticate
        kaggle.api.authenticate()
        kaggle.api.dataset_download_files(USER_NAME + "/" + DATASET_NAME)
        curr_path = os.getcwd()

        with zipfile.ZipFile(
            os.path.join(curr_path,DATASET_NAME + ".zip"),
            "r") as zip_ref:
            zip_ref.extractall("data")

        try:
            for dir in os.listdir(os.path.join(curr_path,"data")):
                if dir.endswith('.csv'):
                    pd.read_csv(os.path.join(curr_path,"data",dir))
        except:
            pass
        return os.path.join(curr_path,"data",dir)


    @task
    def transform_coffee_data(csv_path: str) -> str:
        df = pd.read_csv(csv_path)
        df = df[df['Where do you typically drink coffee? (At a cafe)'] == True]

        output_dict = {}

        for col in DATASET_COLS:
            if col == STRENGTH_COL:
                df[col] = df[col].map(STRENGTH_MAP)
            #unique count of submission id to avoid duplicates
            curr = df.groupby(col, dropna = False)['Submission ID'].agg('nunique')
            if col == FAV_DRINK_COL:
                #sort, top 5
                curr = curr.sort_values(ascending = False)[:5]
            output_dict[col] = curr.to_dict()

        with open(JSON_PATH, 'w') as f:
            json.dump(output_dict, f)
        return JSON_PATH

    @task
    def generate_coffee_report(json_path: str):
        with open(json_path, 'r') as f:
            data = json.load(f)

        
        c = canvas.Canvas(PDF_PATH, pagesize=letter)
        width, height = letter
        c.setFont('Helvetica',18)
        line_height = 200
        image_height = 350
        margin = 10
        current_height = height - margin
        for category, details in data.items():
            if current_height < (margin * 2 + line_height):
                c.showPage()
                current_height = height - margin
            if category in BAR_CHART_COLS:
                image_path = bar_chart(details, category + '.png', category)
            elif category in PIE_CHART_COLS:
                image_path = pie_chart(details, category + '.png', category)
            else:
                continue
            c.drawImage(ImageReader(image_path), margin, current_height - image_height, width=350, height=image_height)
            current_height -= (image_height + margin)

        c.save()


    # task dependencies are defined in a straightforward way
    csv_path = extract_coffee_data()
    json_path = transform_coffee_data(csv_path)
    generate_coffee_report(json_path)


extract_survey_data_dag = extract_survey_data()
