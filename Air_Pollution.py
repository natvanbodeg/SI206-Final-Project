import requests
import sqlite3
import json
import time
from datetime import datetime, timedelta, timezone

def convert_timestamp(timestamp):
    dt_object = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt_object.strftime("%d-%m-%Y")


def create_tables():
    conn = sqlite3.connect('pollution_and_weather_data.db')
    cur = conn.cursor()

    cur.execute('''
    CREATE TABLE IF NOT EXISTS air_pollution (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        latitude REAL,
        longitude REAL,
        aqi INTEGER,
        co REAL,
        no REAL,
        no2 REAL,
        o3 REAL,
        so2 REAL,
        pm2_5 REAL,
        pm10 REAL,
        nh3 REAL,
        timestamp TEXT UNIQUE
    )''')

    conn.commit()
    conn.close()


def store_air_quality_data(data):
    conn = sqlite3.connect('pollution_and_weather_data.db')
    cur = conn.cursor()

    items_to_insert = data.get('list', [])[:25]

    skipped_items = 0

    print(f"Inserting {len(items_to_insert)} new rows.")

    for item in items_to_insert:
        timestamp = item['dt']
        formatted_date = convert_timestamp(timestamp)
        components = item['components']
        aqi = item.get('main', {}).get('aqi')
        
        cur.execute("SELECT COUNT(*) FROM air_pollution WHERE timestamp = ?", (timestamp,))
        count = cur.fetchone()[0]

        if count > 0:
                skipped_items += 1
                print(f"Skipping timestamp {timestamp} as it's already in the database.")
                continue
        try:
            cur.execute('''
            INSERT INTO air_pollution (latitude, longitude, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
            (
                42.3314,
                -83.0458,
                aqi,
                components.get('co'),
                components.get('no'),
                components.get('no2'),
                components.get('o3'),
                components.get('so2'),
                components.get('pm2_5'),
                components.get('pm10'),
                components.get('nh3'),
                formatted_date
            ))

        except sqlite3.IntegrityError as e:
            print(f"Error inserting data: {e}")
            continue

    print(f"Skipping {skipped_items} existing rows.")
    
    conn.commit()
    conn.close()


def get_air_quality_data():

    api_key = '30a0b6c10e81339792cf6acd9d4b75f1'
    lat = 42.3314 
    lon = -83.0458
    start_date = datetime(2021, 1, 1)
    days_to_fetch = 365

    for i in range(days_to_fetch):
        current_start_date = start_date + timedelta(days=i)
        end_date = current_start_date + timedelta(days=1)
        start_timestamp = int(time.mktime(current_start_date.timetuple()))
        end_timestamp = int(time.mktime(end_date.timetuple()))

        url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start_timestamp}&end={end_timestamp}&appid={api_key}"

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            print(f"Fetched data for {current_start_date} to {end_date}: {len(data.get('list', []))} rows")
            store_air_quality_data(data)
        else:
            print(f"Request failed with status code {response.status_code}")

        time.sleep(1)


create_tables()
get_air_quality_data()