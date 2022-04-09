import json
import numpy as np
import pandas as pd
import requests
import sqlite3
import yaml
from requests.exceptions import HTTPError
import time

with open(r'config.yaml') as file:
    config = yaml.load(file, Loader=yaml.FullLoader)

url_discogs_api = "https://api.discogs.com"

try:
    query = {'page': 1, 'per_page': 100}
    url_request = url_discogs_api + "/users/" + config["discogs_user"] + "/collection/folders/0/releases"
    response = requests.get(url_request, params=query)
    response.raise_for_status()
    # access JSOn content
    jsonResponse = response.json()
except HTTPError as http_err:
    print(f'HTTP error occurred: {http_err}')
except Exception as err:
    print(f'Other error occurred: {err}')

no_pages = jsonResponse["pagination"]["pages"]
collection_items = []
for i in range(1, no_pages + 1):
    query = {'page': i, 'per_page': 100}
    url_request = url_discogs_api + "/users/" + config["discogs_user"] + "/collection/folders/0/releases"
    print(requests.get(url_request, params=query))
    response = requests.get(url_request, params=query)
    print(jsonResponse["releases"])
    collection_items.append(jsonResponse["releases"])

print(len(collection_items))

# Read sqlite query results into a pandas DataFrame
#con = sqlite3.connect("~/Development/Datasets/discogs_value.sqlite")
#df = pd.read_sql_query("SELECT * from collection_value", con)

# Verify that result of SQL query is stored in the dataframe
#print(df.head())

#con.close()
# df = pd.read_json('https://api.coinmarketcap.com/v1/ticker/?limit=10')
# r = json.requests.get('http://api.football-data.org/v1/competitions/398/teams')
# x = r.json()
# df = pd.read_json(json.dumps(x))
# 