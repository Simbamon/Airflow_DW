FROM apache/airflow:2.5.1
RUN pip install --user --upgrade pip
RUN pip install apache-airflow-providers-microsoft-mssql
RUN pip install --upgrade google-auth