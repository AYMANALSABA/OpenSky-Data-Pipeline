from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import json
import os
import pandas as pd

default_args = {
    'owner': 'ayman',
    'start_date': datetime(2026, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_opensky_data():
    url = "https://opensky-network.org/api/states/all"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            data = response.json()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"raw_flights_{timestamp}.json"
            target_path = os.path.join('/opt/airflow/data/bronze', filename)
            os.makedirs(os.path.dirname(target_path), exist_ok=True)
            with open(target_path, 'w') as f:
                json.dump(data, f)
    except Exception as e:
        print(f"Error: {e}")


def process_to_silver():
    import pandas as pd
    import os
    import json

    bronze_path = '/opt/airflow/data/bronze'
    silver_path = '/opt/airflow/data/silver'
    os.makedirs(silver_path, exist_ok=True)

    files = [f for f in os.listdir(bronze_path) if f.endswith('.json')]
    if not files: return

    latest_file = os.path.join(bronze_path, sorted(files)[-1])
    with open(latest_file, 'r') as f:
        data = json.load(f)

    states = data.get('states', [])
    columns = ['icao24', 'callsign', 'origin_country', 'time_position', 'last_contact',
               'longitude', 'latitude', 'baro_altitude', 'on_ground', 'velocity',
               'true_track', 'vertical_rate', 'sensors', 'geo_altitude', 'squawk',
               'spi', 'position_source']

    df = pd.DataFrame(states, columns=columns)

    df['velocity'] = pd.to_numeric(df['velocity'], errors='coerce')

    df_clean = df[['icao24', 'callsign', 'origin_country', 'longitude', 'latitude', 'velocity']].copy()
    df_clean = df_clean.dropna(subset=['icao24', 'longitude', 'latitude'])

    df_clean.to_csv(os.path.join(silver_path, 'cleaned_flights.csv'), index=False)

def create_gold_metrics():
    silver_file = '/opt/airflow/data/silver/cleaned_flights.csv'
    gold_path = '/opt/airflow/data/gold'
    os.makedirs(gold_path, exist_ok=True)
    if not os.path.exists(silver_file): return
    df = pd.read_csv(silver_file)
    df_counts = df.groupby('origin_country').size().reset_index(name='flight_count')
    df_counts = df_counts.sort_values(by='flight_count', ascending=False)
    df_counts.to_csv(os.path.join(gold_path, 'flights_per_country.csv'), index=False)

with DAG(
    'opensky_live_tracker',
    default_args=default_args,
    schedule='0 * * * *',
    catchup=False,
) as dag:

    fetch_task = PythonOperator(task_id='fetch_to_bronze', python_callable=fetch_opensky_data)
    silver_task = PythonOperator(task_id='process_to_silver', python_callable=process_to_silver)
    gold_task = PythonOperator(task_id='create_gold_metrics', python_callable=create_gold_metrics)

    fetch_task >> silver_task >> gold_task