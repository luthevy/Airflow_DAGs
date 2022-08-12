"""
    This source is used for initializing Airflow DAGs
"""
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from dataflow_processing import db_to_csv
from airflow.operators.bash import BashOperator
from insert_data import insert_geoloc_table,\
                        insert_sys_table,\
                        insert_weatherdimen_table,\
                        insert_weatherfact_table

with DAG(
    dag_id='init_postgres_db_dag',
    start_date=days_ago(1), 
    schedule_interval="@hourly",
) as dag:
    task1 = PostgresOperator(
        task_id='create_GEOLOC_table',
        postgres_conn_id='postgres_localhost',
        sql="sql/create_GEOLOC.sql"
    )
    task2 = PostgresOperator(
        task_id='create_SYS_table',
        postgres_conn_id='postgres_localhost',
        sql="sql/create_SYS.sql"
    )
    task3 = PostgresOperator(
        task_id='create_WEATHER_table',
        postgres_conn_id='postgres_localhost',
        sql="sql/create_WEATHER.sql"
    )
    task4 = PostgresOperator(
        task_id='create_WEATHERFACT_table',
        postgres_conn_id='postgres_localhost',
        sql="sql/create_WEATHERFACT.sql"
    )
    task5 = PythonOperator(            
        task_id = "load_data",            
        python_callable=db_to_csv
    )
    task6 = insert_geoloc_table()
    task7 = insert_sys_table()
    task8 = insert_weatherdimen_table()
    task9 = insert_weatherfact_table()


    task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7 >> task8 >> task9
