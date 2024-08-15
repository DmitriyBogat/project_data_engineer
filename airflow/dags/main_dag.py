from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import requests
import json
import uuid
from datetime import datetime, timedelta

# Определяем список городов
central_federal_district = [
    "Белгород,Россия",
    "Воронеж,Россия",
    "Курск,Россия",
    "Липецк,Россия",
    "Тамбов,Россия",
]

# Функция для создания таблиц, если их нет
def create_tables(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connect')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Создание таблицы weather_data, если она не существует
    create_weather_data_table = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id VARCHAR PRIMARY KEY,
        load_date VARCHAR,
        city VARCHAR,
        data JSONB,
        processed BOOLEAN DEFAULT FALSE
    );
    """
    
    # Создание таблицы processed_weather_data, если она не существует
    create_processed_weather_data_table = """
    CREATE TABLE IF NOT EXISTS processed_weather_data (
        id VARCHAR PRIMARY KEY,
        date VARCHAR,
        time VARCHAR,
        city VARCHAR,
        temp NUMERIC,
        feelslike NUMERIC,
        humidity NUMERIC,
        dew NUMERIC,
        precip NUMERIC,
        precipprob NUMERIC,
        snow NUMERIC,
        windspeed NUMERIC,
        winddir NUMERIC,
        pressure NUMERIC,
        visibility NUMERIC,
        cloudcover NUMERIC,
        solarradiation NUMERIC,
        solarenergy NUMERIC,
        uvindex NUMERIC,
        conditions VARCHAR(255)
    );
    """

    cursor.execute(create_weather_data_table)
    cursor.execute(create_processed_weather_data_table)
    
    conn.commit()
    cursor.close()
    conn.close()

# Функция для получения погоды и записи в базу данных
def fetch_and_store_weather(**kwargs):
    date_time = datetime.now().strftime("%Y-%m-%dT%H:00:00")
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connect')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    # now = datetime.now()

    
    # start_time = now - timedelta(hours=2)

   
    # current_time = start_time
    # while current_time <= now:
        
    #     time_str = current_time.strftime("%Y-%m-%dT%H:%M:%S")
    #     print(time_str)
        
    
        
    for town in central_federal_district:
        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{town}/{date_time}?key=LF4M3VUUE8JQ62CJPCES9UMTY&include=current&unitGroup=metric&lang=ru"
        response = requests.get(url)
        
        if response.status_code == 200:
            weather_data = response.json()
            
            record_id = str(uuid.uuid4())
            load_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            insert_query = """
            INSERT INTO weather_data (id, load_date, city, data) 
            VALUES (%s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (record_id, load_date, town, json.dumps(weather_data)))
            conn.commit()
        else:
            print(f"Failed to fetch data for {town}. Status code: {response.status_code}")
        # current_time += timedelta(hours=1)
    cursor.close()
    conn.close()

# Функция для обработки данных и записи в таблицу с нужной структурой
def process_weather_data(**kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_connect')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, city, data FROM weather_data WHERE processed IS FALSE")
    rows = cursor.fetchall()

    for row in rows:
        record_id, city, weather_json = row
        current_conditions = weather_json.get("currentConditions", {})
        
        if current_conditions:
            data_id = str(uuid.uuid4())
            date_time = current_conditions.get("datetime", "")
            date = datetime.fromtimestamp(current_conditions.get("datetimeEpoch", 0)).strftime("%Y-%m-%d")
            temp = current_conditions.get("temp", None)
            feelslike = current_conditions.get("feelslike", None)
            humidity = current_conditions.get("humidity", None)
            dew = current_conditions.get("dew", None)
            precip = current_conditions.get("precip", None)
            precipprob = current_conditions.get("precipprob", None)
            snow = current_conditions.get("snow", None)
            windspeed = current_conditions.get("windspeed", None)
            winddir = current_conditions.get("winddir", None)
            pressure = current_conditions.get("pressure", None)
            visibility = current_conditions.get("visibility", None)
            cloudcover = current_conditions.get("cloudcover", None)
            solarradiation = current_conditions.get("solarradiation", None)
            solarenergy = current_conditions.get("solarenergy", None)
            uvindex = current_conditions.get("uvindex", None)
            conditions = current_conditions.get("conditions", "")
            
            insert_query = """
            INSERT INTO processed_weather_data (
                id, date, time, city, temp, feelslike, humidity, dew, precip, 
                precipprob, snow, windspeed, winddir, pressure, visibility, 
                cloudcover, solarradiation, solarenergy, uvindex, conditions
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.execute(insert_query, (
                data_id, date, date_time, city, temp, feelslike, humidity, dew, precip, 
                precipprob, snow, windspeed, winddir, pressure, visibility, 
                cloudcover, solarradiation, solarenergy, uvindex, conditions
            ))

            cursor.execute("UPDATE weather_data SET processed = TRUE WHERE id = %s", (record_id,))
    
    conn.commit()
    cursor.close()
    conn.close()

# Определяем DAG
with DAG(
    dag_id='fetch_and_process_weather_data',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    schedule_interval='@hourly',
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Таска для создания таблиц
    create_tables_task = PythonOperator(
        task_id='create_tables',
        python_callable=create_tables,
        provide_context=True,
    )

    # Таска для получения и сохранения данных погоды
    fetch_weather_task = PythonOperator(
        task_id='fetch_and_store_weather',
        python_callable=fetch_and_store_weather,
        provide_context=True,
    )
    
    # Таска для обработки и сохранения данных в нужной структуре
    process_weather_task = PythonOperator(
        task_id='process_weather_data',
        python_callable=process_weather_data,
        provide_context=True,
    )

    create_tables_task >> fetch_weather_task >> process_weather_task