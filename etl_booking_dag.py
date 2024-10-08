from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd

# Указываем базовый путь для CSV-файлов
base_path = '/opt/airflow/csv_data/'

# Функции загрузки данных
def load_booking_data():
    # Считываем данные и возвращаем в виде JSON строки
    df = pd.read_csv(base_path + 'booking.csv')
    return df.to_json(orient='records')

def load_client_data():
    df = pd.read_csv(base_path + 'client.csv')
    return df.to_json(orient='records')

def load_hotel_data():
    df = pd.read_csv(base_path + 'hotel.csv')
    return df.to_json(orient='records')

# Функция трансформации данных
def transform_data(**kwargs):
    # Получение данных из XCom и преобразование из JSON строки обратно в DataFrame
    booking_data = pd.read_json(kwargs['ti'].xcom_pull(task_ids='load_booking'))
    client_data = pd.read_json(kwargs['ti'].xcom_pull(task_ids='load_client'))
    hotel_data = pd.read_json(kwargs['ti'].xcom_pull(task_ids='load_hotel'))

    # Объединение данных по ключам `client_id` и `hotel_id`
    merged_data = pd.merge(booking_data, client_data, on='client_id', how='left')
    final_data = pd.merge(merged_data, hotel_data, on='hotel_id', how='left')

    # Приведение столбца booking_date к единому формату и преобразование в строку
    final_data['booking_date'] = pd.to_datetime(final_data['booking_date'], errors='coerce').astype(str)

    # Удаление строк, где отсутствуют значения стоимости или валюты бронирования
    final_data = final_data.dropna(subset=['booking_cost', 'currency'])

    # Применение конверсии валют
    conversion_rates = {'GBP': 1.15, 'EUR': 1}
    final_data['booking_cost'] = final_data.apply(
        lambda row: row['booking_cost'] * conversion_rates[row['currency']], axis=1
    )
    final_data['currency'] = 'EUR'

    # Преобразуем итоговый датафрейм в строку JSON для передачи через XCom
    return final_data.to_json(orient='records')

# Функция загрузки в базу данных PostgreSQL
def load_to_db(**kwargs):
    # Получение трансформированных данных из XCom и преобразование обратно в DataFrame
    transformed_data = pd.read_json(kwargs['ti'].xcom_pull(task_ids='transform_data'))
    
    # Подключение к базе данных PostgreSQL
    db_engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    # Сохранение в таблицу 'merged_booking_data'
    transformed_data.to_sql('merged_booking_data', db_engine, if_exists='replace', index=False)

# Определение DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 10, 8),
}

with DAG(
    dag_id='etl_booking_pipeline',
    default_args=default_args,
    description='ETL процесс для объединения данных из нескольких CSV',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Операторы загрузки данных
    load_booking = PythonOperator(
        task_id='load_booking',
        python_callable=load_booking_data,
    )

    load_client = PythonOperator(
        task_id='load_client',
        python_callable=load_client_data,
    )

    load_hotel = PythonOperator(
        task_id='load_hotel',
        python_callable=load_hotel_data,
    )

    # Оператор трансформации данных
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True,
    )

    # Оператор загрузки данных в базу
    load_to_db = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_db,
        provide_context=True,
    )

    # Определение последовательности выполнения
    [load_booking, load_client, load_hotel] >> transform >> load_to_db