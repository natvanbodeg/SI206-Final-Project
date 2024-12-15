import requests
import sqlite3
import json
import time
import os
from datetime import datetime, timedelta, timezone

def reset_database():
    db_path = 'pollution_and_weather_data.db'
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"Database {db_path} has been reset.")
    else:
        print(f"Database {db_path} does not exist.")

def convert_timestamp(timestamp):
    dt_object = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt_object.strftime("%d-%m-%Y %H:%M:%S")


def create_tables():
    conn = sqlite3.connect('pollution_and_weather_data.db')
    cur = conn.cursor()

    cur.execute('''
    CREATE TABLE IF NOT EXISTS air_pollution (
        pollution_id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    seen_timestamps = set()

    print(f"Inserting {len(items_to_insert)} new rows.")

    for item in items_to_insert:
        timestamp = item['dt']
        formatted_date = convert_timestamp(timestamp)

        dt_object = datetime.fromtimestamp(timestamp, tz=timezone.utc)

        hour = dt_object.strftime("%H")

        if hour == "12":
            if formatted_date in seen_timestamps:
                print(f"Duplicate timestamp in API data: {formatted_date}")
                continue
            seen_timestamps.add(formatted_date) 

            cur.execute("SELECT COUNT(*) FROM air_pollution WHERE timestamp = ?", (formatted_date,))
            count = cur.fetchone()[0]

            if count > 0:
                skipped_items += 1
                print(f"Skipping timestamp {formatted_date} as it's already in the database.")
                continue
        
            components = item['components']
            aqi = item.get('main', {}).get('aqi')

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


        print(f"Fetching data from {current_start_date} to {end_date}")

        url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start_timestamp}&end={end_timestamp}&appid={api_key}"

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            print(f"Fetched data for {current_start_date} to {end_date}: {len(data.get('list', []))} rows")
            store_air_quality_data(data)
        else:
            print(f"Request failed with status code {response.status_code}")

        time.sleep(1)


def update_existing_timestamps():
    conn = sqlite3.connect('pollution_and_weather_data.db')
    cur = conn.cursor()
    
    cur.execute("SELECT pollution_id, timestamp FROM air_pollution")
    rows = cur.fetchall()

    updated_rows = 0
    
    for row in rows:
        row_id, timestamp = row

        try:
            numeric_timestamp = int(timestamp)
            readable_date = convert_timestamp(numeric_timestamp)
            cur.execute(
                "SELECT COUNT(*) FROM air_pollution WHERE timestamp = ?",
                (readable_date,)
            )
            if cur.fetchone()[0] == 0:
                cur.execute(
                    "UPDATE air_pollution SET timestamp = ? WHERE pollution_id = ?",
                    (readable_date, row_id)
                )   
                updated_rows += 1
        except ValueError:
            continue
    
    conn.commit()
    conn.close()

    print(f"Updated {updated_rows} rows with readable timestamps.")

if input("Do you want to reset the database? Type 'yes' to confirm: ").lower() == 'yes':
    reset_database()



create_tables()
update_existing_timestamps()
get_air_quality_data()