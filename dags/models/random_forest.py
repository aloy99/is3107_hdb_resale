from airflow.decorators import task, task_group
import functools

from mlflow.tracking import MlflowClient
import mlflow

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_absolute_error
from sklearn.model_selection import train_test_split
from scipy.stats import spearmanr, pearsonr

from reporting.utils import save_plot_as_image, save_table_as_html



    
@task_group(group_id = 'fit_random_forest')
def fit_random_forest(df):

    @task
    def train_model(df, **kwargs):
        # Load data

        y = df[['real_resale_price']]
        X = df.drop(['real_resale_price','town'], axis=1)
        X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.1, shuffle=True, random_state=0)

        # Set tracking url
        mlflow.set_tracking_uri(uri="http://mlflow:5000")
        mlflow.set_experiment(f"Random Forest")

        # Train model
        # Validation using out-of-bag method
        rf = RandomForestRegressor(n_estimators=100, n_jobs = 1, max_depth = 10, oob_score=True, random_state=0)
        rf.fit(X_train, y_train.values.ravel())

        # predict and get evaluation metrics on test set
        predicted_test = rf.predict(X_test)
        oob_test_score = r2_score(y_test['real_resale_price'], predicted_test)
        spearman = spearmanr(y_test['real_resale_price'], predicted_test)
        pearson = pearsonr(y_test['real_resale_price'], predicted_test)
        oob_mae = mean_absolute_error(y_test['real_resale_price'], predicted_test)

        mlflow.log_metric('R2 Score',oob_test_score)
        mlflow.log_metric('Spearman correlation',spearman[0])
        mlflow.log_metric('Pearson correlation',pearson[0])
        mlflow.log_metric('Mean Absolute Error',round(oob_mae))

        metrics = pd.DataFrame(
            index = ['R2 Score', 'Spearman correlation', 'Pearson correlation', 'Mean Absolute Error'],
            data = {'Score': [oob_test_score, spearman[0], pearson[0], oob_mae]})
        save_table_as_html(metrics.style.set_caption("Random Forest Metrics"), 'rf_metrics')
        # Log model to MLFlow
        mlflow.sklearn.log_model(
            sk_model = rf,
            artifact_path = "random_forest_model",
            registered_model_name = "random_forest_model")

    @task
    def scatter_plot_task(df, **kwargs):
        y = df[['real_resale_price']]
        X = df.drop(['real_resale_price','town'], axis=1)
        # Load predictions from MLFlow
        mlflow.set_tracking_uri(uri="http://mlflow:5000")
        model = mlflow.sklearn.load_model(model_uri=f"models:/random_forest_model/latest")
        predictions = model.predict(X)

        # Create scatter plot
        plt.style.use('default')
        fig = plt.figure(figsize=(10,10))
        ax = sns.scatterplot(x=y['real_resale_price'], y=predictions, edgecolors='w', alpha=0.9, s=8)
        ax.set_xlabel('Observed')
        ax.set_ylabel('Predicted')
        ax.set_aspect('equal', adjustable='box')
        temp = [ax.get_ylim(), ax.get_xlim()]
        new_lim = functools.reduce(lambda a, b: tuple([min(a[0],b[0]), max(a[1],b[1])]), temp)
        ax.set_ylim(*new_lim)
        ax.set_xlim(*new_lim)
        save_plot_as_image(plt, 'random_forest_scatter_plot')
        plt.close()

    @task
    def create_feature_importance_plot(df, **kwargs):
        # Load model from MLFlow
        mlflow.set_tracking_uri(uri="http://mlflow:5000")
        model = mlflow.sklearn.load_model(model_uri=f"models:/random_forest_model/latest")

        fig, ax = plt.subplots()
        feat_imp = pd.DataFrame({'Features': df.drop(['real_resale_price','town'], axis=1).columns, 'Feature Importance': model.feature_importances_}).sort_values('Feature Importance', ascending=False)
        sns.barplot(y='Features', x='Feature Importance', data=feat_imp)
        #plt.xticks(rotation=45, ha='right')
        ax.set_title('Out-Of-Bag Feature Importance', size=15)

        save_plot_as_image(plt, 'random_forest_feature_importance')
        plt.close()


    train_model(df) >> scatter_plot_task(df) >> create_feature_importance_plot(df)