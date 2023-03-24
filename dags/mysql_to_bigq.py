import time
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from google.oauth2 import service_account

default_args = {
    'owner': 'simbamon',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args,
    dag_id='etl_mssql_bigquery_dag',
    start_date=datetime(2023, 3, 22),
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=["mssql_to_googlebigq"]
)

# Define Dag Function
def extract_and_load():
    @task()
    def sql_extract():
        try:
            hook = MsSqlHook(mssql_conn_id="ms_sql_server")
            sql = """ select t.name as table_name  
            from sys.tables t where t.name in ('DimReseller') """
            df = hook.get_pandas_df(sql)
            print(df)
            tbl_dict = df.to_dict('dict')
            return tbl_dict
        except Exception as e:
            print("Data extract error: " + str(e))
    @task()
    def gcp_load(tbl_dict: dict):
        try:
            credentials = service_account.Credentials.from_service_account_file('<YOUR_GCP_SA_CREDENTIAL>')
            project_id = "<YOUR_BIGQUERY_PROJECT_NAME>"
            dataset_ref = "<YOUR_BIGQUERY_DATASET_NAME>"

            for value in tbl_dict.values():
                val = value.values()
                for v in val:
                    rows_imported = 0
                    sql = f'select * FROM {v}'
                    hook = MsSqlHook(mssql_conn_id="ms_sql_server")
                    df = hook.get_pandas_df(sql)
                    print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
                    df.to_gbq(destination_table=f'{dataset_ref}.src_{v}', project_id=project_id, credentials=credentials, if_exists="replace")
                    rows_imported += len(df)
        except Exception as e:
            print("Data load error: " + str(e))
    
    tbl_dict = sql_extract()
    tbl_summary = gcp_load(tbl_dict)

gcp_extract_and_load = extract_and_load()