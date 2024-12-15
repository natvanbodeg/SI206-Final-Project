import requests
import sqlite3
from datetime import datetime, timedelta

# Function to fetch weather data for a date range
def fetch_weather_data(api_key, location, start_date, end_date):
    url = "https://api.weatherstack.com/historical"
    params = {
        "access_key": api_key,
        "query": location,
        "historical_date_start": start_date,
        "historical_date_end": end_date
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        if 'historical' in data:
            return data['historical']  # Return only the 'historical' section
        else:
            print("No historical data found in response.")
            return {}
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        return {}

# Find the last stored date in the database
def get_last_stored_date(db_name="pollution_and_weather_data.db"):  # Changed default to pollution_and_weather_data.db to match Air_pollution db 
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Check if the Weather table exists
    cursor.execute("""
    SELECT name FROM sqlite_master WHERE type='table' AND name='Weather';
    """)
    table_exists = cursor.fetchone()

    if not table_exists:  # Table does not exist, so reset to January 1, 2021
        print("Weather table does not exist. Starting from January 1, 2021.")
        conn.close()
        return datetime(2021, 1, 1)

    # If table exists, check if rows are present
    cursor.execute("SELECT COUNT(*) FROM Weather")
    count = cursor.fetchone()[0]

    if count == 0:  # Table is empty
        print("No data found in the table. Starting from January 1, 2021.")
        conn.close()
        return datetime(2021, 1, 1)

    # Query to get the latest date
    cursor.execute("SELECT MAX(date) FROM Weather")
    result = cursor.fetchone()[0]
    conn.close()

    return datetime.strptime(result, "%Y-%m-%d")

# Function to store weather data in SQLite
def store_weather_data_in_db(weather_data, db_name="pollution_and_weather_data.db"):  # Changed default to pollution_and_weather_data.db
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Table schema with an auto-incrementing ID field
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Weather (
        weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT UNIQUE,
        mintemp REAL,
        maxtemp REAL,
        avgtemp REAL,
        totalsnow REAL,
        sunhour REAL,
        uv_index REAL,
        sunrise TEXT,
        sunset TEXT,
        moonrise TEXT,
        moonset TEXT,
        moon_phase TEXT,
        moon_illumination INTEGER
    )
    """)

    inserted_dates = 0  # Counter for newly inserted rows

    # Insert data into the database
    for date, info in weather_data.items():
        try:
            # Extract data fields
            mintemp = info.get('mintemp', None)
            maxtemp = info.get('maxtemp', None)
            avgtemp = info.get('avgtemp', None)
            totalsnow = info.get('totalsnow', None)
            sunhour = info.get('sunhour', None)
            uv_index = info.get('uv_index', None)

            # Astro information
            astro = info.get('astro', {})
            sunrise = astro.get('sunrise', None)
            sunset = astro.get('sunset', None)
            moonrise = astro.get('moonrise', None)
            moonset = astro.get('moonset', None)
            moon_phase = astro.get('moon_phase', None)
            moon_illumination = astro.get('moon_illumination', None)

            # Insert into database (avoid duplicates with UNIQUE constraint)
            cursor.execute("""
            INSERT OR IGNORE INTO Weather (
                date, mintemp, maxtemp, avgtemp, totalsnow, sunhour, uv_index,
                sunrise, sunset, moonrise, moonset, moon_phase, moon_illumination
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (date, mintemp, maxtemp, avgtemp, totalsnow, sunhour, uv_index,
                  sunrise, sunset, moonrise, moonset, moon_phase, moon_illumination))
            
            inserted_dates += 1
        except Exception as e:
            print(f"Error inserting data for {date}: {e}")

    conn.commit()
    conn.close()
    print(f"Inserted {inserted_dates} new rows into the database.")

# Main execution
if __name__ == "__main__":
    # Replace with your actual API key
    WEATHER_API_KEY = "0b6eb37ce8265b86413953582aa20ee8"
    location = "Detroit, United States"

    # Find the last stored date and calculate the next 25 days
    last_date = get_last_stored_date(db_name="pollution_and_weather_data.db")  # Explicitly pass the new database name
    start_date = last_date  # Don't add a day if it's the first run
    end_date = start_date + timedelta(days=24)  # Fetch 25 days

    # Format dates for API
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Fetch and store weather data
    print(f"Fetching weather data from {start_date_str} to {end_date_str}...")
    weather_data = fetch_weather_data(WEATHER_API_KEY, location, start_date_str, end_date_str)

    if weather_data:
        store_weather_data_in_db(weather_data, db_name="pollution_and_weather_data.db")  # Explicitly pass the new database name
        print("Process completed successfully.")
    else:
        print("No weather data found.")