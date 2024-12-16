import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime

# Load the data from the calculated results file
timestamps = []
avg_temperatures = []
avg_pollution_levels = []

with open("calculated_results.txt", "r") as f:
    # Skip the header line
    next(f)
    
    # Read the data
    for line in f:
        parts = line.strip().split("\t")
        timestamps.append(parts[0])  # timestamps as strings
        avg_temperatures.append(float(parts[1]))
        avg_pollution_levels.append(float(parts[2]))

# Convert timestamps to datetime objects
timestamps = [datetime.strptime(ts, "%Y-%m-%d %H:%M:%S") for ts in timestamps]

# 1. Scatter Plot: Average Temperature vs. Average Pollution Level
plt.figure(figsize=(10, 6))
plt.scatter(avg_temperatures, avg_pollution_levels, color='blue', alpha=0.5)
plt.title("Average Temperature vs. Average Pollution Level")
plt.xlabel("Average Temperature (°C)")
plt.ylabel("Average Pollution Level")
plt.grid(True)
plt.savefig("scatter_plot.png")  # Save the scatter plot as a PNG file
plt.show()

# 2. Line Plot: Average Temperature and Average Pollution Level over Time
plt.figure(figsize=(10, 6))

# Line plot for average temperature
plt.plot(timestamps, avg_temperatures, label="Average Temperature (°C)", color='red', marker='o')

# Line plot for average pollution level
plt.plot(timestamps, avg_pollution_levels, label="Average Pollution Level", color='green', marker='x')

# Set the title and labels
plt.title("Average Temperature and Pollution Level Over Time")
plt.xlabel("Timestamp")
plt.ylabel("Values")
plt.xticks(rotation=45)  # Rotate x-axis labels for better readability

# Format the x-axis labels to only show day, month, and year
plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

# Set the x-axis ticks to appear at a specific interval (e.g., every 5th day)
plt.gca().xaxis.set_major_locator(mdates.DayLocator(interval=5))

plt.legend()
plt.grid(True)
plt.tight_layout()

# Save and show the plot
plt.savefig("line_plot_formatted.png")  # Save the line plot as a PNG file
plt.show()
