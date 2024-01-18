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
from airflow.sensors.filesystem import FileSensor

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

    csv_file_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'retail_data/files/')  # get file dir
    all_csv_files = [os.path.join(csv_file_dir, csv_file) for csv_file in os.listdir(csv_file_dir)] # all csv files
    print(f"Total CSV files: {len(all_csv_files)}")

    # create a job
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV
                                        , skip_leading_rows=1
                                        , autodetect=False
                                        , write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                                        )
    
    for csv_file_path in all_csv_files:
        table_name = csv_file_path.split('/')[-1].split('.')[0]  # exract table_name from CSV
        with open(csv_file_path, "rb") as source_file:
            try:
                  job = client.load_table_from_file(source_file
                                                , f"{Variable.get('gcp_project')}.{Variable.get('gcp_bigquery_retail_dataset')}.{table_name}"
                                                , job_config=job_config)
                  job.result()  # Waits for the job to complete
                  print(f"Data insertion is successful in {table_name} table.")
            except:
                 print(f"Something is wrong with {csv_file_path} file and {table_name} table.")
    
    """
    GET request from bigQuery table
    """
    #table = client.get_table(f"{Variable.get('gcp_project')}.{Variable.get('gcp_bigquery_retail_dataset')}.actor")  # Make an API request.
    #print("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), f"{Variable.get('gcp_project')}.{Variable.get('gcp_bigquery_retail_dataset')}.actor"))

with DAG(dag_id='retail_data'
     , tags=['gcp_bigquery', 'dag_optimization', 'retail_data']
     , default_args=default_args
     , description='test'
     , start_date=datetime(2024, 1, 18)
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
            
            #2.1: create actor table
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
            
            
            #2.2: create address table
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
            

            #2.3:create category table
            create_category_table = BigQueryCreateEmptyTableOperator(task_id='create_category_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='category',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                        {"name": "catgory_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            

            #2.4:create city table
            create_city_table = BigQueryCreateEmptyTableOperator(task_id='create_city_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='city',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                        {"name": "city_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "city", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "country_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            #2.5:create country table
            create_country_table = BigQueryCreateEmptyTableOperator(task_id='create_country_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='country',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                        {"name": "country_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "country", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            
            #2.6:create customer table
            create_customer_table = BigQueryCreateEmptyTableOperator(task_id='create_customer_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='customer',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                        {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "store_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "address_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "activebool", "type": "BOOLEAN", "mode": "NULLABLE"},
                                                                        {"name": "create_date", "type": "DATE", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
                                                                        {"name": "active", "type": "INTEGER", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            
            #2.7: create film table
            create_film_table = BigQueryCreateEmptyTableOperator(task_id='create_film_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='film',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                        {"name": "film_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "title", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "description", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "release_year", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "language_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "rental_duration", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "rental_rate", "type": "FLOAT", "mode": "NULLABLE"},
                                                                        {"name": "length", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "replacement_cost", "type": "FLOAT", "mode": "NULLABLE"},
                                                                        {"name": "rating", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"},
                                                                        {"name": "special_features", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "full_text", "type": "STRING", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            
            #2.8: create film_actor table
            create_film_actor_table = BigQueryCreateEmptyTableOperator(task_id='create_film_actor_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='film_actor',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                        {"name": "actor_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "film_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            
            #2.9: create film_category table
            create_film_category_table = BigQueryCreateEmptyTableOperator(task_id='create_film_category_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='film_category',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                       {"name": "film_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "category_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            
            #2.10: create inventory table
            create_inventory_table = BigQueryCreateEmptyTableOperator(task_id='create_inventory_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='inventory',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                       {"name": "inventory_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "film_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "store_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            #2.11: create language table
            create_language_table = BigQueryCreateEmptyTableOperator(task_id='create_language_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='language',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                       {"name": "language_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            
            #2.12: create payment table
            create_payment_table = BigQueryCreateEmptyTableOperator(task_id='create_payment_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='payment',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                       {"name": "payment_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "staff_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "rental_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "amount", "type": "FLOAT", "mode": "NULLABLE"},
                                                                        {"name": "payment_date", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            
            #2.13: create rental table
            create_rental_table = BigQueryCreateEmptyTableOperator(task_id='create_rental_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='rental',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                       {"name": "rental_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "rental_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                                                                       {"name": "inventory_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "customer_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "return_date", "type": "TIMESTAMP", "mode": "NULLABLE"},
                                                                       {"name": "staff_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            
            #2.14: create staff table
            create_staff_table = BigQueryCreateEmptyTableOperator(task_id='create_staff_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='staff',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                        {"name": "staff_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "first_name", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "last_name", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "address_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "store_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                        {"name": "active", "type": "BOOLEAN", "mode": "NULLABLE"},
                                                                        {"name": "username", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "password", "type": "STRING", "mode": "NULLABLE"},
                                                                        {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        #{"name": "picture", "type": "STRING", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            
            #2.15: create store table
            create_store_table = BigQueryCreateEmptyTableOperator(task_id='create_store_table',
                                                                  gcp_conn_id='my_gcp_conn',
                                                                  project_id='{{ var.value.gcp_project}}',
                                                                  dataset_id='{{ var.value.gcp_bigquery_retail_dataset }}',
                                                                  table_id='store',
                                                                  if_exists='ignore',
                                                                  schema_fields=[
                                                                       {"name": "store_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "manager_staff_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "address_id", "type": "INTEGER", "mode": "NULLABLE"},
                                                                       {"name": "last_update", "type": "TIMESTAMP", "mode": "NULLABLE"}
                                                                        ]
                                                                        )
            
            # taskflow for table creation
            # ==========================
            [create_actor_table, create_address_table, create_category_table, create_city_table, \
             create_country_table, create_customer_table, create_film_table, create_film_actor_table, \
             create_film_category_table, create_inventory_table, create_language_table, create_payment_table, \
             create_rental_table, create_staff_table, create_store_table]
    
    #3. check file is available
    with TaskGroup(group_id='csv_file_available', 
                   tooltip='check files are available') as is_csv_file_available:
        
        # 3.1: actor csv file availability check
        is_actor_csv_file_available = FileSensor(task_id='is_actor_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='actor.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        # 3.2: address csv file availability check
        is_address_csv_file_available = FileSensor(task_id='is_address_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='address.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        # 3.3: category csv file availability check
        is_category_csv_file_available = FileSensor(task_id='is_category_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='category.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        # 3.4: city csv file availability check
        is_city_csv_file_available = FileSensor(task_id='is_city_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='city.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        # 3.5: country csv file availability check
        is_country_csv_file_available = FileSensor(task_id='is_country_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='country.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        # 3.6: customer csv file availability check
        is_customer_csv_file_available = FileSensor(task_id='is_customer_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='customer.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        # 3.7: film csv file availability check
        is_film_csv_file_available = FileSensor(task_id='is_film_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='film.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        # 3.8: film_actor csv file availability check
        is_film_actor_csv_file_available = FileSensor(task_id='is_film_actor_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='film_actor.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        # 3.9: film_category csv file availability check
        is_film_category_csv_file_available = FileSensor(task_id='is_film_category_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='film_category.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        #3.10: inventory csv file availability check
        is_inventory_csv_file_available = FileSensor(task_id='is_inventory_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='inventory.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        #3.11: _language_ csv file availability check
        is_language_csv_file_available = FileSensor(task_id='is_language_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='language.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        #3.12: payment csv file availability check
        is_payment_csv_file_available = FileSensor(task_id='is_payment_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='payment.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        #3.13: rental csv file availability check
        is_rental_csv_file_available = FileSensor(task_id='is_rental_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='rental.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        #3.14: staff csv file availability check
        is_staff_csv_file_available = FileSensor(task_id='is_staff_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='staff.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        #3.15: store csv file availability check
        is_store_csv_file_available = FileSensor(task_id='is_store_csv_file_available',
                                        fs_conn_id='my_csv_file', 
                                        filepath='store.csv',
                                        mode='poke', 
                                        timeout=30, 
                                        poke_interval=5)
        
        # taskflow for file checking availability
        [is_actor_csv_file_available, is_address_csv_file_available, is_category_csv_file_available, \
         is_city_csv_file_available, is_country_csv_file_available, is_customer_csv_file_available, \
         is_film_csv_file_available, is_film_actor_csv_file_available, is_film_category_csv_file_available, \
         is_inventory_csv_file_available, is_language_csv_file_available, is_payment_csv_file_available, \
         is_rental_csv_file_available, is_staff_csv_file_available, is_store_csv_file_available]
    
    #4. insert data from CSV
    insert_data_from_csv = PythonOperator(task_id='insert_data_from_csv',
                                       python_callable=_get_data_from_csv)
    
    #5. Run dbt
    run_dbt = BashOperator(task_id='run_dbt'
                           , bash_command='cd ~/projects/project-dbt/dbt-practice/dashingDwhBigquery && /home/samrat/projects/project-dbt/dbt-practice/bin/dbt run')
    
    #6. Run soda
    run_soda = BashOperator(task_id='run_soda'
                           , bash_command='cd ~/projects/soda-projects/soda-basic && ~/projects/soda-projects/soda-basic/bin/soda scan -d dbt_retail_data -c configuration.yml checks.yml')

    # Taskflow
    create_retail_data_dataset >> Label('create or skip dataset') >> create_table_group \
          >> Label('create/skip table') >> is_csv_file_available >> Label('check file availability') \
              >> insert_data_from_csv >> Label('fetch data and insert into BigQuery') >> run_dbt \
              >> Label('run dbt & run soda') >> run_soda
