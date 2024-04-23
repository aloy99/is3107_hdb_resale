from airflow.decorators import task, task_group
import functools

from mlflow.tracking import MlflowClient
import mlflow

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm

from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from reporting.utils import save_plot_as_image, save_table_as_html



    
@task_group(group_id = 'fit_linear_regression')
def fit_linear_regression(df):

    @task
    def train_model(df, **kwargs):
        # Load data
        lr_y = df[['real_resale_price']]
        lr_X = df.drop(['real_resale_price','town'], axis=1)
        
        y = df[['real_resale_price']]
        X = df.drop(['real_resale_price','town'], axis=1)
        X_train, X_test, y_train, y_test = train_test_split(X,y, test_size = 0.1, shuffle=True, random_state=0)


        # Set tracking url
        mlflow.set_tracking_uri(uri="http://mlflow:5000")
        mlflow.set_experiment(f"Linear Regression")

        # Train model
        X_constant = sm.add_constant(X_train)
        lin_reg = sm.OLS(np.log(y_train), X_constant.astype(float)).fit()

        # Log model to MLFlow
        y_pred = lin_reg.predict(sm.add_constant(X_test).astype(float))
        r2 = r2_score(np.log(y_test), y_pred)
        n = sm.add_constant(X_test).shape[0]
        p = sm.add_constant(X_test).shape[1]
        adj_r2 = 1-(1-r2)*(n-1)/(n-p-1)
        mlflow.log_metric("Adjusted R2", 1-(1-r2)*(n-1)/(n-p-1))
        metrics = pd.DataFrame(index = ['Adjusted R2'], data = {'Score': [adj_r2]})
        save_table_as_html(metrics.style.set_caption("Linear Regression Metrics"), 'lr_metrics')
        mlflow.statsmodels.log_model(
            statsmodels_model = lin_reg,
            artifact_path = "lr_model",
            registered_model_name = "lr_model")

    @task
    def scatter_plot_task(df, **kwargs):
        lr_y = df[['real_resale_price']]
        lr_X = df.drop(['real_resale_price','town'], axis=1)
        # Load predictions from MLFlow
        mlflow.set_tracking_uri(uri="http://mlflow:5000")
        model = mlflow.statsmodels.load_model(model_uri=f"models:/lr_model/latest")
        predictions = model.predict(sm.add_constant(lr_X).astype(float))

        # Create scatter plot
        plt.style.use('default')
        fig = plt.figure(figsize=(10,10))
        ax = sns.scatterplot(x=np.log(lr_y)['real_resale_price'], y=predictions, edgecolors='w', alpha=0.9, s=8)
        ax.set_xlabel('Observed')
        ax.set_ylabel('Predicted')
        ax.set_aspect('equal', adjustable='box')
        temp = [ax.get_ylim(), ax.get_xlim()]
        new_lim = functools.reduce(lambda a, b: tuple([min(a[0],b[0]), max(a[1],b[1])]), temp)
        ax.set_ylim(*new_lim)
        ax.set_xlim(*new_lim)
        ax.annotate('Adjusted R\u00b2: ' + str(format(round(model.rsquared_adj,2),'.2f')), xy=(0, 1), xytext=(25, -25),
                    xycoords='axes fraction', textcoords='offset points', fontsize=12)
        save_plot_as_image(plt, 'linear_regression_scatter_plot')
        plt.close()

    @task
    def create_feature_importance_plot(**kwargs):
        # Load model from MLFlow
        mlflow.set_tracking_uri(uri="http://mlflow:5000")
        model = mlflow.statsmodels.load_model(model_uri=f"models:/lr_model/latest")

        # Create feature importance plot
        lr_results = pd.read_html(model.summary().tables[1].as_html(),header=0,index_col=0)[0]
        coefs = lr_results[['coef']][1:].reset_index().rename(columns={'index':'feature'})
        coefs['feature_importance'] = np.abs(coefs['coef'])
        coefs = coefs.sort_values('feature_importance').reset_index(drop=True)
        coefs['color'] = coefs['coef'].apply(lambda x: '#66ff8c' if x>0 else '#ff8c66')
        coefs.plot.barh(x='feature',y='feature_importance',color=coefs['color'],figsize=(10,5))
        colors = {'Positive':'#66ff8c', 'Negative':'#ff8c66'}
        labels = list(colors.keys())
        handles = [plt.Rectangle((0,0),1,1, color=colors[label]) for label in labels]
        legend = plt.legend(handles, labels, title='Relationship', fontsize = '15')
        plt.setp(legend.get_title(),fontsize='17')
        plt.xlabel('Standardized Coefficients', size=15), plt.ylabel('Features', size=15)
        plt.ylim([-1,23])
        plt.title('Linear Regression Feature Importance', size=15)
        save_plot_as_image(plt, 'linear_regression_feature_importance')
        plt.close()


    train_model(df) >> scatter_plot_task(df) >> create_feature_importance_plot()