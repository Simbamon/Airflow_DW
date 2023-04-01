import time
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from google.oauth2 import service_account

default_args = {
    'owner': 'simbamon',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@task()
def get_src_tables():
    hook = MsSqlHook(mssql_conn_id="ms_sql_server")
    sql = """select  t.name as table_name  
     from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductCategory') """
    df = hook.get_pandas_df(sql)
    print(df)
    tbl_dict = df.to_dict('dict')
    return tbl_dict

@task()
def load_src_data(tbl_dict: dict):
    try:
        credentials = service_account.Credentials.from_service_account_file('<YOUR_GCP_SA_CREDENTIAL>')
        project_id = "<YOUR_BIGQUERY_PROJECT_NAME>"
        dataset_ref = "<YOUR_BIGQUERY_DATASET_NAME>"
        all_tbl_name = []

        for k, v in tbl_dict['table_name'].items():
            all_tbl_name.append(v)
            rows_imported = 0   
            sql = f'select * FROM {v}'
            hook = MsSqlHook(mssql_conn_id="ms_sql_server")
            df = hook.get_pandas_df(sql)
            df['LargePhoto'] = df['LargePhoto'].astype('str') 
            print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
            df.to_gbq(destination_table=f'{dataset_ref}.src_{v}', project_id=project_id, credentials=credentials, if_exists="replace")
            rows_imported += len(df)
    except Exception as e:
        print("Data load error: " + str(e))

# Transformation tasks
# Will update more later
@task()
def transform_srcProduct():
    try:
        credentials = service_account.Credentials.from_service_account_file('<YOUR_GCP_SA_CREDENTIAL>')
        project_id = "<YOUR_BIGQUERY_PROJECT_NAME>"
        dataset_ref = "<YOUR_BIGQUERY_DATASET_NAME>"    
    except Exception as e:
        print("Data load error: " + str(e))

with DAG(
    default_args=default_args,
    dag_id='etl_mssql_bigquery_dag_v2',
    start_date=datetime(2023, 3, 29),
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=["bigquery_test"]
) as dag:
    
    with TaskGroup("extract_dimProudcts_load", tooltip="Extract and load source data") as extract_load_src_bigq:
        src_product_tbls = get_src_tables()
        load_dimProducts = load_src_data(src_product_tbls)
        #define order
        src_product_tbls >> load_dimProducts

    
    extract_load_src_bigq