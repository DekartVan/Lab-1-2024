import os
import datetime
import pandas as pd
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Определение директорий
INPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'input')
OUTPUT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'data', 'output')
TEMP_FILE = 'intermediate.csv'

# Функция чтения файлов
def read_and_concat_files(**kwargs):
    files = [os.path.join(INPUT_PATH, f'chunk{i}.csv') for i in range(26)]
    dataframes = [pd.read_csv(file) for file in files]
    combined_df = pd.concat(dataframes, ignore_index=True)
    combined_df.to_csv(TEMP_FILE, index=False)

# Функция фильтрации данных
def filter_non_null_rows(**kwargs):
    df = pd.read_csv(TEMP_FILE)
    df = df[df['designation'].notna() & df['region_1'].notna()]
    df.to_csv(TEMP_FILE, index=False)

# Функция замены null значений
def replace_null_values(**kwargs):
    df = pd.read_csv(TEMP_FILE)
    df.fillna({'price': 0.0}, inplace=True)
    df.to_csv(TEMP_FILE, index=False)

# Функция сохранения результата в CSV
def save_to_csv(**kwargs):
    final_df = pd.read_csv(TEMP_FILE)
    final_df.to_csv(os.path.join(OUTPUT_PATH, 'final_output.csv'), index=False)

# Функция сохранения в Elasticsearch
def save_to_elasticsearch(**kwargs):
    df = pd.read_csv(TEMP_FILE)
    es_client = Elasticsearch("http://localhost:15601/")
    for _, row in df.iterrows():
        es_client.index(index='lab1_final', body=row.to_json())

# Определение DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

with DAG(
    dag_id='data_processing_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    read_files_task = PythonOperator(
        task_id='read_and_concat_files',
        python_callable=read_and_concat_files
    )
    
    filter_rows_task = PythonOperator(
        task_id='filter_non_null_rows',
        python_callable=filter_non_null_rows
    )
    
    replace_null_task = PythonOperator(
        task_id='replace_null_values',
        python_callable=replace_null_values
    )
    
    save_csv_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv
    )
    
    save_es_task = PythonOperator(
        task_id='save_to_elasticsearch',
        python_callable=save_to_elasticsearch
    )
    
    # Задаем порядок выполнения задач
    read_files_task >> filter_rows_task >> replace_null_task >> [save_csv_task, save_es_task]
