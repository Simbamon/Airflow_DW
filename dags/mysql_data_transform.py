import time
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from google.oauth2 import service_account

default_args = {
    'owner': 'simbamon',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

dag_variable_config = Variable.get("gcp_variables_config", deserialize_json=True)
credentials_config = dag_variable_config["service_account_credential"]
project_config = dag_variable_config["project_id"]
dataset_config = dag_variable_config["dataset_ref"]

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
        credentials = service_account.Credentials.from_service_account_file(credentials_config)
        project_id = project_config
        dataset_ref = dataset_config
        all_tbl_name = []
        start_time = time.time()

        for k, v in tbl_dict['table_name'].items():
            all_tbl_name.append(v)
            print(all_tbl_name)
            rows_imported = 0   
            sql = f'select * FROM {v}'
            hook = MsSqlHook(mssql_conn_id="ms_sql_server")
            df = hook.get_pandas_df(sql)
            if (all_tbl_name == ['DimProduct']):
                df['LargePhoto'] = df['LargePhoto'].astype('str') 
            print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
            df.to_gbq(destination_table=f'{dataset_ref}.src_{v}', project_id=project_id, credentials=credentials, if_exists="replace")
            rows_imported += len(df)
            print(f'Done. {str(round(time.time() - start_time, 2))} total seconds elapsed')
    except Exception as e:
        print("Data load error: " + str(e))

# Transformation tasks
# Will update more later
@task()
def transform_srcProduct():
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_config)
        project_id = project_config
        dataset_ref = dataset_config
        src_dimprod = f"""
        SELECT * FROM `{project_config}.{dataset_config}.src_DimProduct` LIMIT 1000
        """
        pdf = pd.read_gbq(src_dimprod, project_id=project_id, credentials=credentials)
        print(pdf)
        revised = pdf[['ProductKey', 'ProductAlternateKey', 'ProductSubcategoryKey','WeightUnitMeasureCode', 'SizeUnitMeasureCode', 'EnglishProductName',
                   'StandardCost','FinishedGoodsFlag', 'Color', 'SafetyStockLevel', 'ReorderPoint','ListPrice', 'Size', 'SizeRange', 'Weight',
                   'DaysToManufacture','ProductLine', 'DealerPrice', 'Class', 'Style', 'ModelName', 'EnglishDescription', 'StartDate','EndDate', 'Status']]
        #replace nulls
        revised['WeightUnitMeasureCode'].fillna('0', inplace=True)
        revised['ProductSubcategoryKey'].fillna('0', inplace=True)
        revised['SizeUnitMeasureCode'].fillna('0', inplace=True)
        revised['StandardCost'].fillna('0', inplace=True)
        revised['ListPrice'].fillna('0', inplace=True)
        revised['ProductLine'].fillna('NA', inplace=True)
        revised['Class'].fillna('NA', inplace=True)
        revised['Style'].fillna('NA', inplace=True)
        revised['Size'].fillna('NA', inplace=True)
        revised['ModelName'].fillna('NA', inplace=True)
        revised['EnglishDescription'].fillna('NA', inplace=True)
        revised['DealerPrice'].fillna('0', inplace=True)
        revised['Weight'].fillna('0', inplace=True)
        revised = revised.rename(columns={"EnglishDescription": "Description", "EnglishProductName":"ProductName"})
        revised = revised.astype(str)
        # revised.to_sql(f'stg_DimProduct', engine, if_exists='replace', index=False)
        revised.to_gbq(destination_table=f'{dataset_ref}.stg_DimProdut',project_id=project_id, credentials=credentials, if_exists="replace")
        return {"table(s) processed ": "Data imported successful"}

    except Exception as e:
        print("Data load error: " + str(e))

@task()
def transform_srcProductSubcategory():
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_config)
        project_id = project_config
        dataset_ref = dataset_config
        src_prodsubcat = f"""
        SELECT * FROM `{project_config}.{dataset_config}.src_DimProductSubcategory` LIMIT 1000
        """
        pdf = pd.read_gbq(src_prodsubcat, project_id=project_id, credentials=credentials)
        revised = pdf[['ProductSubcategoryKey','EnglishProductSubcategoryName', 'ProductSubcategoryAlternateKey', 'ProductCategoryKey']]
        revised = revised.rename(columns={"EnglishProductSubcategoryName": "ProductSubcategoryName"})
        revised.to_gbq(destination_table=f'{dataset_ref}.stg_DimProductSubcategory',project_id=project_id, credentials=credentials, if_exists="replace")
        return {"table(s) processed ": "Data imported successful"}
    except Exception as e:
        print("Data load error: " + str(e))

@task()
def transform_srcProductCategory():
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_config)
        project_id = project_config
        dataset_ref = dataset_config
        src_dimprodcat = f"""
        SELECT * FROM `{project_config}.{dataset_config}.src_DimProductCategory` LIMIT 1000
        """
        pdf = pd.read_gbq(src_dimprodcat, project_id=project_id, credentials=credentials)
        print(pdf)
        revised = pdf[['ProductCategoryKey', 'ProductCategoryAlternateKey','EnglishProductCategoryName']]
        revised = revised.rename(columns={"EnglishProductCategoryName": "ProductCategoryName"})
        revised.to_gbq(destination_table=f'{dataset_ref}.stg_DimProductCategory',project_id=project_id, credentials=credentials, if_exists="replace")
        return {"table(s) processed ": "Data imported successful"}
    except Exception as e:
        print("Data load error: " + str(e))

@task()
def prdProduct_model():
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_config)
        project_id = project_config
        dataset_ref = dataset_config
        stg_dimprodcat = f"""
        SELECT * FROM `{project_config}.{dataset_config}.stg_DimProductCategory` LIMIT 1000
        """
        stg_dimprod = f"""
        SELECT * FROM `{project_config}.{dataset_config}.stg_DimProdut` LIMIT 1000
        """
        stg_dimprodsubcat = f"""
        SELECT * FROM `{project_config}.{dataset_config}.stg_DimProductSubcategory` LIMIT 1000
        """
        pc = pd.read_gbq(stg_dimprodcat, project_id=project_id, credentials=credentials)
        p = pd.read_gbq(stg_dimprod, project_id=project_id, credentials=credentials)
        p['ProductSubcategoryKey'] = p.ProductSubcategoryKey.astype(float)
        p['ProductSubcategoryKey'] = p.ProductSubcategoryKey.astype(int)
        ps = pd.read_gbq(stg_dimprodsubcat, project_id=project_id, credentials=credentials)
        merged = p.merge(ps, on='ProductSubcategoryKey').merge(pc, on='ProductCategoryKey')
        merged.to_gbq(destination_table=f'{dataset_ref}.prd_DimProductCategory',project_id=project_id, credentials=credentials, if_exists="replace")
        return {"table(s) processed ": "Data imported successful"}
    except Exception as e:
        print("Data load error: " + str(e))

with DAG(
    default_args=default_args,
    dag_id='etl_mssql_bigquery_dag_real',
    start_date=datetime(2023, 4, 3),
    schedule_interval='0 10 * * *',
    catchup=False,
    tags=["bigquery_test_real"]
) as dag:
    
    with TaskGroup("extract_dimProudcts_load", tooltip="Extract and load source data in BigQuery") as extract_load_src_bigq:
        src_product_tbls = get_src_tables()
        load_dimProducts = load_src_data(src_product_tbls)
        #define order
        src_product_tbls >> load_dimProducts

    with TaskGroup("transform_src_product", tooltip="Transform and stage data in BigQuery") as transform_src_product_bigq:
        transform_srcProduct = transform_srcProduct()
        transform_srcProductSubcategory = transform_srcProductSubcategory()
        transform_srcProductCategory = transform_srcProductCategory()        
        [transform_srcProduct, transform_srcProductSubcategory, transform_srcProductCategory]
    
    with TaskGroup("load_product_model", tooltip="Final Product model in BigQuery") as load_product_model_bigq:
        prd_Product_model = prdProduct_model()
        prd_Product_model

    # extract_load_src_bigq >> transform_src_product_bigq >> load_product_model_bigq
    extract_load_src_bigq >> transform_src_product_bigq