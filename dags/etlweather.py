from airflow import DAG 
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import json
import requests

#latitude and longitude for desired location (Example: London)
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

#deafault arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

#step1 : define the DAG
with DAG(dag_id = 'weather_etl_pipeline' ,
         default_args = default_args,
         schedule_interval = '@daily',
         catchup = False) as dags:

    #step 2: define the tasks

    @task
    def extract_weather_data():
        """Extract weather data from Open-Meteo API using Airflow connection."""
        
        #use HttpHook to get connection details from Airflow connections
        http_hook = HttpHook(http_conn_id=API_CONN_ID , method='GET')

        ##Build the API endpoint
        ##https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"


        ##Make the request via HttpHook
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            weather_data = response.json()
            return weather_data
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")
        
    @task
    def transform_weather_data(weather_data):
        """Transform the extracted weather data to get relevant fields."""
        current_weather = weather_data.get('current_weather')
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'], 
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    
    @task
    def load_weather_data(transformed_data):
        """Load the transformed weather data into Postgres database."""
        
        #use PostgresHook to connect to Postgres
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        #create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                latitude VARCHAR(50),
                longitude VARCHAR(50),
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        #insert data into table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode, recorded_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    ##DAG Workflow : ETL Pipeline
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)






