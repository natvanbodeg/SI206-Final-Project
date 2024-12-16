import sqlite3
from datetime import datetime

# Function to standardize timestamps
def standardize_timestamp(timestamp):
    try:
        # If the timestamp is in 'YYYY-MM-DD' format (without time), add a default time of '00:00:00'
        if len(timestamp) == 10:  # Format: 'YYYY-MM-DD'
            timestamp += " 00:00:00"
        
        # Attempt to parse various timestamp formats
        if "T" in timestamp:  # ISO 8601 format
            dt = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S")
        else:  # Other common format, e.g., "YYYY-MM-DD HH:MM:SS"
            dt = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        
        return dt.strftime("%Y-%m-%d %H:%M:%S")  # Return standardized format
    except ValueError:
        print(f"Unable to parse timestamp: {timestamp}")
        return None

# Connect to the SQLite database
conn = sqlite3.connect("pollution_and_weather_data.db")  # Ensure the correct DB name
cursor = conn.cursor()

# Ensure tables exist (you can omit this if you're confident they exist)
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='Weather'")
if cursor.fetchone() is None:
    print("Weather table does not exist!")
    conn.close()
    exit()

cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='air_pollution'")
if cursor.fetchone() is None:
    print("air_pollution table does not exist!")
    conn.close()
    exit()

# Fetch raw data from both tables
cursor.execute("SELECT id, date, avgtemp FROM Weather")  # Assuming column names based on your Weather.py code
Weather = cursor.fetchall()

cursor.execute("SELECT id, timestamp, aqi FROM air_pollution")  # Assuming column names based on your air_pollution.py code
air_pollution = cursor.fetchall()

# Standardize timestamps in-memory
weather_data_cleaned = [(id_, standardize_timestamp(ts), temp) for id_, ts, temp in Weather]
pollution_data_cleaned = [(id_, standardize_timestamp(ts), aqi) for id_, ts, aqi in air_pollution]

# Create temporary in-memory tables for cleaned data
cursor.execute("DROP TABLE IF EXISTS temp_weather")
cursor.execute("DROP TABLE IF EXISTS temp_pollution")

cursor.execute("""
    CREATE TEMP TABLE temp_weather (id INTEGER, timestamp TEXT, temperature REAL)
""")
cursor.execute("""
    CREATE TEMP TABLE temp_pollution (id INTEGER, timestamp TEXT, pollution_level REAL)
""")

# Insert cleaned data into temporary tables
cursor.executemany("INSERT INTO temp_weather VALUES (?, ?, ?)", weather_data_cleaned)
cursor.executemany("INSERT INTO temp_pollution VALUES (?, ?, ?)", pollution_data_cleaned)

# Perform the JOIN and calculate the average temperature and pollution per timestamp
query = """
    SELECT tw.timestamp, AVG(tw.temperature) AS avg_temp, AVG(tp.pollution_level) AS avg_pollution
    FROM temp_weather tw
    JOIN temp_pollution tp ON tw.id = tp.id
    GROUP BY tw.timestamp
"""
cursor.execute(query)
results = cursor.fetchall()

# Write results to a text file
with open("calculated_results.txt", "w") as f:
    f.write("Timestamp\tAverage Temperature\tAverage Pollution Level\n")
    for row in results:
        # Use 0.00 or another placeholder if value is None
        avg_temp = row[1] if row[1] is not None else 0.00
        avg_pollution = row[2] if row[2] is not None else 0.00
        f.write(f"{row[0]}\t{avg_temp:.2f}\t{avg_pollution:.2f}\n")

print("Results written to 'calculated_results.txt'.")
# Close the database connection
conn.close()
