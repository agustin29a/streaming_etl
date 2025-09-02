from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from helpers.my_utilities import (
    cargar_datos_s3,
    create_tables_with_constraints,
    get_db_engine,
    limpiar_diccionario,
    convertir_dataframes_a_parquet_s3
)
from helpers.metadata import dataframe_metadata

# Funciones ETL
def extract(**context):
    dataframes = cargar_datos_s3()
    context["ti"].xcom_push(key="raw_data", value=dataframes)


def transform(**context):
    dataframes = context["ti"].xcom_pull(key="raw_data", task_ids="extract_task")
    dataframe_str = limpiar_diccionario(dataframes)
    context["ti"].xcom_push(key="clean_data", value=dataframe_str)


def load_parquet(**context):
    dataframe_str = context["ti"].xcom_pull(key="clean_data", task_ids="transform_task")
    convertir_dataframes_a_parquet_s3(dataframe_str)


def load_db(**context):
    dataframe_str = context["ti"].xcom_pull(key="clean_data", task_ids="transform_task")
    engine = get_db_engine()

    # Metadatos de tablas (archivo aparte)
    create_tables_with_constraints(dataframe_str, dataframe_metadata, engine)


# Definición del DAG
with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2025, 8, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "postgres", "airflow"],
) as dag:

    extract_task = PythonOperator(task_id="extract_task", python_callable=extract)

    transform_task = PythonOperator(task_id="transform_task", python_callable=transform)

    load_task_parquet = PythonOperator(task_id="load_task_parquet", python_callable=load_parquet)
    
    load_task_db = PythonOperator(task_id="load_task_db", python_callable=load_db)

    # Dependencias
    extract_task >> transform_task >> load_task_parquet >> load_task_db
