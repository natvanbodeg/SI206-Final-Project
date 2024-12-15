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

# Function to store weather data directly in SQLite
def store_weather_data_in_db(weather_data, db_name="weather_data2.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Updated table schema with an auto-incrementing ID field
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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

    # Insert data from API response directly
    for date, info in weather_data.items():
        try:
            # Extract data fields for the date
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

            # Insert into database (avoid duplicates with UNIQUE constraint on date)
            cursor.execute("""
            INSERT OR IGNORE INTO Weather (
                date, mintemp, maxtemp, avgtemp, totalsnow, sunhour, uv_index,
                sunrise, sunset, moonrise, moonset, moon_phase, moon_illumination
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (date, mintemp, maxtemp, avgtemp, totalsnow, sunhour, uv_index,
                  sunrise, sunset, moonrise, moonset, moon_phase, moon_illumination))
        except Exception as e:
            print(f"Error inserting data for {date}: {e}")

    conn.commit()
    conn.close()
    print("Weather data successfully stored in the database.")

# Main execution
if __name__ == "__main__":
    # Replace with your actual API key
    WEATHER_API_KEY = "0b6eb37ce8265b86413953582aa20ee8"

    # Parameters
    location = "Detroit, United States"

    # Set dynamic start and end dates (January 1, 2020, for 25 days)
    start_date = datetime(2021, 1, 1)
    end_date = start_date + timedelta(days=24)  


    # Format dates for API
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")

    # Fetch weather data directly from API
    print(f"Fetching weather data from {start_date_str} to {end_date_str}...")
    weather_data = fetch_weather_data(WEATHER_API_KEY, location, start_date_str, end_date_str)

    # Store the weather data in SQLite
    if weather_data:
        store_weather_data_in_db(weather_data)
        print("Process completed successfully.")
    else:
        print("No weather data found.")