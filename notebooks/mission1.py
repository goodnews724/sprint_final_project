from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
from datetime import datetime
import pandas as pd
import io
import os, pendulum

default_args = dict(
    owner='hyunsoo',
    email=['hyunsoo@airflow.com'],
    email_on_failure=False,
    retries=3
)

def read_parquet_from_gcs(gcs_path: str, credentials_path=None, engine: str = "pyarrow") -> pd.DataFrame:
    """
    GCS에 저장된 Parquet 파일을 pandas DataFrame으로 읽어옵니다.

    Parameters:
    - gcs_path: str — GCS 파일 경로 (예: "gs://my-bucket/folder/file.parquet")
    - engine: str — 사용할 파케이 엔진 ("pyarrow" 또는 "fastparquet")

    Returns:
    - pd.DataFrame
    """
    if credentials_path:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    
    df = pd.read_parquet(gcs_path, engine=engine)
    return df

# pip install pandas pyarrow gcsfs 가 다운되면 사용가능

with DAG(
    dag_id = 'load_parquet',
    start_date = pendulum.datetime(2025, 5, 1, tz='Asia/Seoul'),
    schedule="30 * * * *", # cron 표현식
    tags = ['20250515'],
    default_args = default_args,
    catchup=False
):
    
    your_gcs_parquet_path = "gs://your-gbucket-name/your-folder/your-file.parquet"
    your_credentials_path = "/opt/airflow/dags/key.json" 

    read_parquet = PythonOperator(
        task_id="read_parquet",
        python_callable=read_parquet_from_gcs,
        op_args=[your_gcs_parquet_path, your_credentials_path]
    )