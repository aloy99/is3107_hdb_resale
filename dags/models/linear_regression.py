from airflow.decorators import task, task_group

from mlflow.tracking import MlflowClient
import mlflow

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm

from reporting.utils import save_plot_as_image



    
@task_group(group_id = 'fit_linear_regression')
def fit_linear_regression(df):

    @task
    def train_model(df, **kwargs):
        # Load data
        lr_y = df[['real_resale_price']]
        lr_X = df.drop(['real_resale_price','town'], axis=1)


        # Set tracking url
        mlflow.set_tracking_uri(uri="http://mlflow:5000")
        mlflow.set_experiment(f"Linear Regression")

        # Train model
        X_constant = sm.add_constant(lr_X)
        lin_reg = sm.OLS(np.log(lr_y), X_constant).fit()
        predictions = lin_reg.predict()

        # Log model to MLFlow
        mlflow.log_metric("adjusted_r_squared", lin_reg.rsquared_adj)
        mlflow.log_model(lin_reg, "model")
        mlflow.log_metric("predictions", predictions)

    @task
    def scatter_plot_task(df, **kwargs):
        lr_y = df[['real_resale_price']]
        
        # Load predictions from MLFlow
        client = MlflowClient()
        predictions = client.get_metric("predictions")

        # Create scatter plot
        plt.style.use('default')
        fig = plt.figure(figsize=(5,3))
        ax = sns.scatterplot(x=np.log(lr_y)['real_price'], y=predictions, edgecolors='w', alpha=0.9, s=8)
        ax.set_xlabel('Observed')
        ax.set_ylabel('Predicted')
        ax.annotate('Adjusted R\u00b2: ' + str(format(round(lin_reg.rsquared_adj,2),'.2f')), xy=(0, 1), xytext=(25, -25),
                    xycoords='axes fraction', textcoords='offset points', fontsize=12)
        save_plot_as_image(plt, 'linear_regression_scatter_plot')
        plt.close()

    @task
    def create_feature_importance_plot(**kwargs):
        # Load model from MLFlow
        client = MlflowClient()
        model = client.get_latest_versions("model", stages=["Production"])[0].source

        # Create feature importance plot
        lr_results = pd.read_html(model.summary().tables[1].as_html(),header=0,index_col=0)[0]
        coefs = lr_results[['coef']][1:].reset_index().rename(columns={'index':'feature'})
        coefs['feature_importance'] = np.abs(coefs['coef'])
        coefs = coefs.sort_values('feature_importance').reset_index(drop=True)
        coefs['color'] = coefs['coef'].apply(lambda x: '#66ff8c' if x>0 else '#ff8c66')
        coefs.plot.barh(x='feature',y='feature_importance',color=coefs['color'],figsize=(4,5))
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