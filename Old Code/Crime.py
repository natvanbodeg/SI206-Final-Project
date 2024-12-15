import requests
import sqlite3
import json

def create_tables():
    conn = sqlite3.connect('crime_weather_data.db')
    cur = conn.cursor()

    cur.execute('''
    CREATE TABLE IF NOT EXISTS fbi_crimes (id INTEGER PRIMARY KEY, crime_type TEXT, location TEXT, date_time TEXT, severity TEXT)''')
    
    conn.commit()
    conn.close()

create_tables()


def fetch_fbi_crime_data():
    api_key = '9HpgzPtjuhJLcRaGh0y9rfxXOOHf2zShEfWES8fQ'
    url = 'https://api.usa.gov/crime/fbi/cde/'

    params = {
        'api_key': api_key,
    }



    try:
        response = requests.get(url)
        data = response.json()
        return data
    
    except:
        return None
    
data = fetch_fbi_crime_data()
print(data)

