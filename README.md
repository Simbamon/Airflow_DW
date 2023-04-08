# Creating ETL for BigQuery

## Generating Keys from GCP
Making Service Account on Google Cloud Platform:
  1. Hit the main menu on the top left corner and navigate to `IAM & Admin`
  2. Click `Service Accounts` in the `IAM & Admin` submenu
  3. In the Service Accounts menu on top, click `CREATE SERVICE ACCOUNT`
  4. Fill out the information
      - Service account name: Name of the service account
      - Service account ID: Automatically generated after you create the name
      - Service account description: Description of the service account  
     
     Then click `CREATE AND CONTINUE`
  5. Grant the access to project on the Service Account you just created
      These access are based on the BigQuery ETL project
      - BigQuery Admin
      - BigQuery User
      - BigQuery Connection Service Agent
  6. You can skip the last configuration part for this project

Creating new Key for Service Account
  1. Click the ellipsis on the right side of the Service Account you are going to use
  2. Click `Manage keys` on the drop-down menu
  3. In the Keys page, click `ADD KEY` then click `Create new key`
  4. There are two options:
      - JSON (Recommended)
      - P12 (For backward compatibility with code using the P12 format)  
    
     Click `JSON`
  5. After the creation, the json key will automatically start downloading to your machine

## Notes
How to access Airflow CLI through docker
  1. grab the CONTAINER ID through `docker ps`
  2. run this command
     ```
     docker exec -it {AIRFLOW_CONTAINER_ID} bash
     ```

How to add Environment Vairables in Airflow
  1. Prepare JSON file (Example)
      ```json
        {
          "gcp_variables_config": {
              "service_account_credential":"YOUR_SERVICE_ACCOUNT_CREDENTIAL",
              "project_id":"YOUR_PROJECT_ID",
              "gcp_source":"YOUR_SOURCE_NAME",
              "gcp_staging":"YOUR_STAGING_NAME",
              "gcp_production":"YOUR_PRODUCTION_NAME"
          }
        }
      ```
  2. Go to Admin -> Variable and choose the JSON file to import variables
  3. In Dag, import Variable library from airflow.models (Example)
     ```python
     from airflow.models import Variable
     ```

## FIXES
1.  Unable to convert dataframe to parquet error in BigQuery
    Some of the columns in MS AdventureWorkDW2019 were different data type than usual
    For instance, SalesTerritoryImage data type is varbinary and looks like it is causing an error
    To fix this, reference this [stackoverflow case](https://stackoverflow.com/questions/64364129/unable-to-convert-dataframe-to-parquet-typeerror)
    Example:
    ```python
    df['SalesTerritoryImage'] = df['SalesTerritoryImage'].astype('str') 
    ```
