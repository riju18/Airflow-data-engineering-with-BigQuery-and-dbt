import os
import pandas as pd
from airflow.models import DAG
from airflow.models import Variable
from datetime import timedelta, datetime
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery  # bigquery ops
from google.oauth2 import service_account  # service account credentials
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator,\
    BigQueryCreateEmptyTableOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'samrat',
    'email_on_failure': False,
    'email_on_retry': False,
    'email_on_success': False,
    'email': 'samrat.mitra@vivasoftltd.com',
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

def _get_data_from_csv():
    """
    fetch data from csv and then bulk upload to bigQuery table
    """

    credentials = service_account.Credentials.from_service_account_file(Variable.get('gcp_account'))
    client = bigquery.Client(credentials=credentials
                             , project=Variable.get('gcp_project'))

    csv_file_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'retail_data/files/actor.csv')

    # create a job
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV
                                        , skip_leading_rows=1
                                        , autodetect=False
                                        , write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
        )
    
    with open(csv_file_path, "rb") as source_file:  # open csv file
         job = client.load_table_from_file(source_file
                                           , f"{Variable.get('gcp_project')}.{Variable.get('gcp_bigquery_retail_dataset')}.actor"
                                           , job_config=job_config)
         
    job.result()  # Waits for the job to complete.
    
    table = client.get_table(f"{Variable.get('gcp_project')}.{Variable.get('gcp_bigquery_retail_dataset')}.actor") # get a table
    print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), f"{Variable.get('gcp_project')}.{Variable.get('gcp_bigquery_retail_dataset')}.actor"))

with DAG(dag_id='retail_data'
     , tags=['gcp_bigquery', 'dag_optimization', 'retail_data']
     , default_args=default_args
     , description='test'
     , start_date=datetime(2024, 1, 13)
     , schedule='@daily'  # only once
     , catchup=False):
    
    #1. create Dataset ops
    create_retail_data_dataset = BigQueryCreateEmptyDatasetOperator(task_id='create_retail_data_dataset',
                                                                    gcp_conn_id='my_gcp_conn',
                                                                    project_id='{{ var.value.gcp_project}}',
                                                                    dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                    if_exists='ignore'
                                                        )
    
    #2. create Table ops
    with TaskGroup(group_id='create_table_group'
                   ,tooltip='table creation group') as create_table_group:
            
            """
            create actor table
            """
            create_actor_table = BigQueryCreateEmptyTableOperator(task_id='create_actor_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='actor',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                        {"name": "actor_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            """
            create address table
            """
            create_address_table = BigQueryCreateEmptyTableOperator(task_id='create_address_table',
                                                                    gcp_conn_id='my_gcp_conn',
                                                                    project_id='{{ var.value.gcp_project}}',
                                                                    dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                    table_id='address',
                                                                    if_exists='ignore',
                                                                    schema_fields=[
                                                                          {"name": "address_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                          {"name": "address", "type": "STRING", "mode": "NULLABLE"},
                                                                          {"name": "address2", "type": "STRING", "mode": "NULLABLE"},
                                                                          {"name": "district", "type": "STRING", "mode": "NULLABLE"},
                                                                          {"name": "city_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                          {"name": "postal_code", "type": "STRING", "mode": "NULLABLE"},
                                                                          {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
                                                                          {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}     
                                                                    ]
                                                                    )
            
            [create_actor_table, create_address_table]
    
    #3. insert data from CSV
    insert_data_from_csv = PythonOperator(task_id='insert_data_from_csv',
                                       python_callable=_get_data_from_csv)
    
    #4. Run dbt
    run_dbt = BashOperator(task_id='run_dbt'
                           , bash_command='cd dbtProjectDir && dbtVenv/dbt run')

    # Taskflow
    create_retail_data_dataset >> create_table_group >> \
        insert_data_from_csv >> run_dbt
