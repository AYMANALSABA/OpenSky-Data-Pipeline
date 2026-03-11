import pandas as pd


def clean_data(raw_data):
    print("Transforming data...")
    columns = ['icao24', 'callsign', 'origin_country', 'time_position',
               'last_contact', 'longitude', 'latitude', 'baro_altitude',
               'on_ground', 'velocity', 'true_track', 'vertical_rate',
               'sensors', 'geo_altitude', 'squawk', 'spi', 'position_source']

    df = pd.DataFrame(raw_data, columns=columns)

    # Keep only 5 useful columns
    df = df[['callsign', 'origin_country', 'longitude', 'latitude', 'velocity']]

    # Drop rows with missing location
    df = df.dropna(subset=['longitude', 'latitude'])

    return df