# Importing the required libraries 
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
import json 
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd 


'''
Defining a python functioon to convert kelvin to fahrenheit
'''
def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit


'''
Transforming the Data received from API and Saving into csv on my local PC
'''
def _transformation(ti):
    data = ti.xcom_pull(task_ids='extract_api_data')
    city = data['name']
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,                       
                        }
    transformed_data_list = [transformed_data]
    df = pd.DataFrame(transformed_data_list)
    df.to_csv('/opt/airflow/airflow_data/transform_weather_data.csv')
    
    


default_args = {
    'owner': 'salman',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 24),
    'retries': 2,
#     'retry_delay': timedelta(minutes=1)
}


# Initialiing the DAG
with DAG('weather_etl', default_args=default_args,
        schedule_interval='@daily', catchup=False) as dag:
    
    # Sensor -- Creating a is_api_available sensor to wait until an API available
    is_api_available= HttpSensor(
        task_id = 'is_api_available',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=pune&appid=829e977b8fc0898758a4d19d0fac66b2',
    )
    
    # Once the API is ready then extracting the data from the API using json Http Operator
    extract_api_data = SimpleHttpOperator(
        task_id = 'extract_api_data',
        http_conn_id = 'weathermap_api',
        endpoint='/data/2.5/weather?q=pune&appid=829e977b8fc0898758a4d19d0fac66b2',
        method = 'GET',
        response_filter= lambda r: json.loads(r.text),
        log_response=True
    )
    
    # Transforming the Data and Loading data in my local pc in csv format using pandas
    transform_load_data= PythonOperator(
        task_id='transform_load_data',
        python_callable=_transformation
    )
    
    
    # Defining the dependencies or workflow
    is_api_available >> extract_api_data >> transform_load_data
