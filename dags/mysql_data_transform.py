import time
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd

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