from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator

from airflow.sdk import dag, task

@dag(schedule=None, description="A  DAG that runs manually")
def dag_data_processing():
    @task


    @task



dag_data_processing()
