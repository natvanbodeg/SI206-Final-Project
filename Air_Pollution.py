import requests
import sqlite3
import json
import os
import time
from datetime import datetime, timedelta, timezone

DB_PATH = 'pollution_and_weather_data.db'
STATE_FILE = 'last_processed_date.txt'

def convert_timestamp(timestamp):
    """Convert a UNIX timestamp to a formatted date string."""
    dt_object = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt_object.strftime("%Y-%m-%d %H:%M:%S")

def create_tables():
    """Create the air_pollution table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
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

def store_air_quality_data(date, data):
    """
    Store air quality data in the database. If no data is returned for 12 PM, insert null values.
    """
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    seen_timestamps = set()

    formatted_noon = f"{date.strftime('%Y-%m-%d')} 12:00:00"

    if data:
        # Filter for data only at 12 PM
        noon_data = [item for item in data.get('list', []) 
                     if datetime.fromtimestamp(item['dt'], tz=timezone.utc).strftime("%H") == "12"]

        if noon_data:
            item = noon_data[0]
            formatted_date = convert_timestamp(item['dt'])
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
                print(f"Inserted data for {formatted_date}")
            except sqlite3.IntegrityError:
                print(f"Skipped duplicate timestamp: {formatted_date}")
            seen_timestamps.add(formatted_date)

    # Insert NULL values if no valid data was found for noon
    if formatted_noon not in seen_timestamps:
        try:
            cur.execute('''
            INSERT INTO air_pollution (latitude, longitude, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
            (
                42.3314,
                -83.0458,
                None, None, None, None, None, None, None, None, None, formatted_noon
            ))
            print(f"Noon data missing. Inserted NULL values for {formatted_noon}")
        except sqlite3.IntegrityError:
            print(f"Skipped duplicate NULL entry for {formatted_noon}")

    conn.commit()
    conn.close()

def get_last_processed_date():
    """Retrieve the last processed date or use the default start date."""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return datetime.strptime(f.read().strip(), "%Y-%m-%d")
    else:
        # Default start date if state file doesn't exist
        return datetime(2021, 1, 1)

def save_last_processed_date(last_date):
    """Save the last processed date to the state file."""
    with open(STATE_FILE, 'w') as f:
        f.write(last_date.strftime("%Y-%m-%d"))

def get_air_quality_data():
    """Fetch air quality data from the API and insert it into the database."""
    api_key = '30a0b6c10e81339792cf6acd9d4b75f1'
    lat = 42.3314
    lon = -83.0458

    start_date = get_last_processed_date()
    print(f"Fetching data starting from: {start_date.strftime('%Y-%m-%d')}")

    days_fetched = 0
    while days_fetched < 25:  # Limit to 25 days per run
        current_date = start_date + timedelta(days=days_fetched)
        next_date = current_date + timedelta(days=1)

        start_timestamp = int(current_date.replace(tzinfo=timezone.utc).timestamp())
        end_timestamp = int(next_date.replace(tzinfo=timezone.utc).timestamp())

        print(f"Fetching data for {current_date.strftime('%Y-%m-%d')}...")

        url = (f"http://api.openweathermap.org/data/2.5/air_pollution/history"
               f"?lat={lat}&lon={lon}&start={start_timestamp}&end={end_timestamp}&appid={api_key}")

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            store_air_quality_data(current_date, data)
        else:
            print(f"Request failed with status code {response.status_code} for {current_date.strftime('%Y-%m-%d')}")
            # Insert NULL values if request fails
            store_air_quality_data(current_date, None)

        days_fetched += 1
        time.sleep(1)  # Be polite to the API

    # Update the state file to track the next starting date
    save_last_processed_date(start_date + timedelta(days=25))
    print(f"Finished processing up to: {start_date + timedelta(days=24)}")

if __name__ == "__main__":
    create_tables()
    get_air_quality_data()
