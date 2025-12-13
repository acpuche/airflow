import json
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

LOCAL_TZ = "America/Bogota"

with DAG(
    """Definición del DAG para ingesta de datos del dataset de viajes mensuales
     de NYC TLC (Taxi & Limousine Comission).

    """
    dag_id="dag_data_processing",
    schedule="@monthly",
    start_date=pendulum.datetime(2025,1,1, tz=LOCAL_TZ),
    description="Ingesta mensual de datos del dataset NYC TLC",
    catchup=False,
    tags=["tlc","lambda","aws"],
    max_consecutive_failed_dag_runs=3
) as dag:
    
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    dataset = "yellow_tripdata"

    #Payloads
    lambda_payload_dict = {
    """Set the parameters for downloading the dataset from the website and copying 
    it to Landing
    """
        "dataset" : dataset,
        "date" : "{{ data_interval_start.strftime('%Y-%m') }}",
        "s3_folder" : "landing",
        "bucket" : "aura-puche-data-lake",
        "url" : "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ data_interval_start.strftime('%Y-%m') }}.parquet"
    }
    lambda_payload = json.dumps(lambda_payload_dict)

    glue_raw_to_processed_payload_dict = {
        "dataset" : dataset,
        "date" : "{{ data_interval_start.strftime('%Y-%m') }}",
        "ds" : "{{ data_interval_start.strftime('%Y-%m-%d') }}",
        "s3_source_folder" : "raw/yellow_tripdata",
        "s3_target_folder" : "processed/yellow_tripdata",
        "bucket" : "aura-puche-data-lake",
        "glue_job_name" : "glue-raw-to-processed-job"
    }
    glue_raw_to_processed_payload = json.dumps(glue_raw_to_processed_payload_dict)

    #Tasks

    lambda_ingest = LambdaInvokeFunctionOperator(
    """Lambda ingestion for one month, rwites to landing zone in S3
    """
        task_id="dataset_extraction",
        aws_conn_id="aws_connection",
        region_name="us-east-2",
        function_name="DockerLambdaAwsStack-DockerFuncF47DA747-V8kJFKF8Jhz6",
        log_type="Tail",
        invocation_type="RequestResponse",
        payload=lambda_payload
    )

    glue_landing_to_raw = GlueJobOperator(
    """Glue job to move data from landing to raw zone in S3
    """
        task_id="glue_landing_to_raw",
        job_name="glue-landing-to-raw-job",
        script_location="s3://aura-puche-glue-scripts/glue_landing_to_raw.py",
        aws_conn_id="aws_connection",
        region_name="us-east-2",
        job_desc="Glue job to move data from landing to raw zone in S3",
        wait_for_completion=True,
        create_job_kwargs={
            "GlueVersion": "3.0",
            "NumberOfWorkers": 2,
            "WorkerType": "G.1X",
        },
        script_args={"--payload": glue_raw_to_processed_payload}
    )    

    start >> lambda_ingest >> glue_landing_to_raw >> end