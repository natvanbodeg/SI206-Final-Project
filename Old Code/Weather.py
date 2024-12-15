#Name: Taylor Yonkman
#Student ID: 12921683
# Email: tayloryo@umich.edu
# List who you have worked with on this project: Natalie Vandobeg
# List any AI tool (e.g. ChatGPT, GitHub Copilot): ChatGPT

import requests
import sqlite3
import json

def fetch_weather_data():
    url = "https://api.weatherstack.com/historical?access_key={0b6eb37ce8265b86413953582aa20ee8}"

    querystring = {"query":"Michigan", "historical_date_start":"2015-01-01", "historical_date_end":"2015-03-01"}

    response = requests.get(url, params=querystring)

    print(response.json())
