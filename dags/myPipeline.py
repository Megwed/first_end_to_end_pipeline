from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from google.cloud import storage
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 15),
}

# Define the DAG
dag = DAG(
    'my_first_data_pileline',
    default_args=default_args,
    schedule_interval=None,
)

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to GCS only if it does not already exist."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Check if the file already exists in the bucket
    if blob.exists():
        return
    blob.upload_from_filename(source_file_name)

# Define the function to store the data in a pandas dataframe
def store_data_in_dataframe(**kwargs):
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    query = "SELECT * FROM olympics"
    df = pd.read_sql(query, engine)
    output_csv_path = '/opt/airflow/dags/olympics_data.csv'   
    df.to_csv(output_csv_path, index=False)
    print(f"Data has been written to {output_csv_path}")
    kwargs['ti'].xcom_push(key='csv_file_path', value=output_csv_path)    

# Sensor to check if the table exists
check_table_exists = BigQueryTableExistenceSensor(
    task_id="check_table_exists",
    project_id="ready-de-25",
    dataset_id="airflow_star_schema",
    table_id="olympics",
    dag=dag,
)

# Task 1: Export data from Postgres to CSV
export_data_task = PostgresOperator(
    task_id='export_data_to_csv',
    sql="""
    COPY (SELECT * FROM olympics) TO '/var/lib/postgresql/data/olympics_data.csv' DELIMITER ',' CSV HEADER;
    """,
    postgres_conn_id='postgres_default',
    dag=dag,
)

# Task 2: Store the data from the Postgres table into a Pandas DataFrame
store_data_task = PythonOperator(
    task_id='store_data_in_dataframe',
    python_callable=store_data_in_dataframe,
    provide_context=True,  
    dag=dag,
)

# Task 3: Upload the CSV file to GCS using the custom Python function
upload_to_gcs_task = PythonOperator(
    task_id='upload_to_gcs',
    python_callable=upload_to_gcs,
    op_kwargs={
        'bucket_name': 'ready-d25-postgres-to-gcs',
        'source_file_name': '/opt/airflow/dags/olympics_data.csv',  
        'destination_blob_name': 'abdelgawad/olympics_data.csv',
    },
    dag=dag,
)


# Task 4: Create an empty table in BigQuery
create_table = BigQueryCreateEmptyTableOperator(
    task_id="create_table_olympics",
    project_id="ready-de-25",
    dataset_id="airflow_star_schema",
    table_id="olympics",
    schema_fields=[
    {"name": "player_id", "mode": "NULLABLE", "type": "INTEGER"},
    {"name": "name", "mode": "NULLABLE", "type": "STRING"},
    {"name": "sex", "mode": "NULLABLE", "type": "STRING"},
    {"name": "team", "mode": "NULLABLE", "type": "STRING"},
    {"name": "noc", "mode": "NULLABLE", "type": "STRING"},
    {"name": "year", "mode": "NULLABLE", "type": "INTEGER"},
    {"name": "season", "mode": "NULLABLE", "type": "STRING"},
    {"name": "city", "mode": "NULLABLE", "type": "STRING"},
    {"name": "sport", "mode": "NULLABLE", "type": "STRING"},
    {"name": "event", "mode": "NULLABLE", "type": "STRING"},
    {"name": "medal", "mode": "NULLABLE", "type": "STRING"}
],
    dag=dag,
)

# Use schema in GCSToBigQueryOperator
load_csv_to_bigquery = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery",
    bucket="ready-d25-postgres-to-gcs",
    source_objects=["abdelgawad/olympics_data.csv"],
    destination_project_dataset_table="airflow_star_schema.olympics",
    schema_fields=[
    {"name": "player_id", "mode": "NULLABLE", "type": "INTEGER"},
    {"name": "name", "mode": "NULLABLE", "type": "STRING"},
    {"name": "sex", "mode": "NULLABLE", "type": "STRING"},
    {"name": "team", "mode": "NULLABLE", "type": "STRING"},
    {"name": "noc", "mode": "NULLABLE", "type": "STRING"},
    {"name": "year", "mode": "NULLABLE", "type": "INTEGER"},
    {"name": "season", "mode": "NULLABLE", "type": "STRING"},
    {"name": "city", "mode": "NULLABLE", "type": "STRING"},
    {"name": "sport", "mode": "NULLABLE", "type": "STRING"},
    {"name": "event", "mode": "NULLABLE", "type": "STRING"},
    {"name": "medal", "mode": "NULLABLE", "type": "STRING"}
],
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

# Set task dependencies
export_data_task >> store_data_task >> upload_to_gcs_task >> check_table_exists >> create_table >> load_csv_to_bigquery


