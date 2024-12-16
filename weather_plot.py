import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime

# Function to fetch date and high temperature data from the database
def fetch_high_temperatures(db_name="pollution_and_weather_data.db"):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Query to get the date and maxtemp from the Weather table
    cursor.execute("SELECT date, maxtemp FROM weather ORDER BY date ASC")
    data = cursor.fetchall()
    conn.close()

    # Process the data: extract dates and temperatures
    dates = [datetime.strptime(row[0], "%Y-%m-%d") for row in data]  # Convert date strings to datetime objects
    maxtemps = [row[1] for row in data if row[1] is not None]  # Ignore None values for maxtemp

    return dates, maxtemps

# Function to plot high temperatures
def plot_high_temperatures(dates, maxtemps):
    plt.figure(figsize=(10, 6))  # Set the figure size
    plt.plot(dates, maxtemps, marker='o', linestyle='-', color='g', label='High Temperature (°C)')  # Line plot

    # Add labels and title
    plt.title("High Temperatures Over Time")
    plt.xlabel("Date")
    plt.ylabel("High Temperature (°C)")
    plt.grid(True)  # Add gridlines
    plt.xticks(rotation=45)  # Rotate x-axis labels for readability
    plt.tight_layout()  # Adjust layout to prevent clipping of labels
    plt.legend()  # Add legend

    # Show the plot
    plt.show()

# Main execution
if __name__ == "__main__":
    # Fetch data from the database
    db_name = "pollution_and_weather_data.db"  # Your database name
    dates, maxtemps = fetch_high_temperatures(db_name)

    # Plot the data
    if dates and maxtemps:
        plot_high_temperatures(dates, maxtemps)
    else:
        print("No data available to plot.")
