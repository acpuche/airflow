from airflow import DAG
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator

with DAG(
    schedule="@monthly",
    start_date=pendulum.datetime(2025,5,1, tz=LOCAL_TZ)
    description="Ingesta mensual de datos del dataset NYC TLC",
    catchup="False"
    tags=["tlc","lambda","aws"]
    max_consecutive_failed_dag_runs=3
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    dataset = "yellow_tripdata"

    #Payloads
    # Downloads to Landing
    lambda_payload_dict = {
        "dataset" : dataset,
        "date" : "{{ data_interval_start.strftime('%Y-%m') }}",
        "s3_folder" : "landing",
        "bucket" : "aura-puche-data-lake",
        "url" : "https://d37ci6vzurychx.cloudfront.net/trip-data/{{ dataset }}_{{ data_interval_start.strftime('%Y-%m') }}.parquet"
    }
    lambda_payload = json.dumps(lambda_payload_dict)

    #Tasks
    #Lambda ingestion 
    dataset_extraction="dataset_extraction"
    aws_conn_id="aws_connection"
    region_name="us-east-2"
    function_name="DockerLambdaAwsStack-DockerFuncF47DA747-V8kJFKF8Jhz6"
    log_type="Tail"
    innvocation_type="RequestResponse"
    payload=lambda_payload 