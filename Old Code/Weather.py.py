
import requests
import json

url = "https://api.weatherstack.com/historical?access_key=0b6eb37ce8265b86413953582aa20ee8"

querystring = {"query":"Detroit, United States", "historical_date_start":"2015-01-01", "historical_date_end":"2015-02-28"}

response = requests.get(url, params=querystring)

if response.status_code == 200:
    data = response.json()  # Parse the JSON response
    
    # Save the JSON response to a file
    with open("jan-feb_mi.json", "w") as json_file:
        json.dump(data, json_file, indent=4)  # `indent=4` makes the file readable
    print("Data saved to jan-feb_mi.json")
else:
    print(f"Failed to retrieve data: {response.status_code}")