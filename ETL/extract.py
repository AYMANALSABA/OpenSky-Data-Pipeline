import requests
import config

def get_live_data():
    print("Extracting data from OpenSky...")
    response = requests.get(config.URL)
    if response.status_code == 200:
        return response.json().get('states', [])
    return []