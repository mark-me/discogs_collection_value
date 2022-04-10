import datetime as dt
import json
import numpy as np
import pandas as pd
import requests
import sqlite3
import yaml
from requests.exceptions import HTTPError
import time

def main():

    with open(r'config.yaml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    url_discogs_api = "https://api.discogs.com"
    name_discogs_user = config["discogs_user"]
    db_file = config["db_file"]
    df_collection = retrieve_collection_items(name_discogs_user, url_discogs_api)
    store_collection_items(df_collection, db_file)
    df_collection_value = retrieve_lowest_value(df_collection, url_discogs_api)
    store_lowest_value(df_collection_value, db_file)

def retrieve_collection_items(name_discogs_user, url_discogs_api):

    # Get first page to get the number of pages
    try:
        query = {'page': 1, 'per_page': 100}
        url_request = url_discogs_api + "/users/" + name_discogs_user + "/collection/folders/0/releases"
        response = requests.get(url_request, params=query)
        response.raise_for_status()
        jsonResponse = response.json()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')

    no_pages = jsonResponse["pagination"]["pages"] # * 50

    # Retrieving all collection items
    collection_items = []
    for i in range(1, no_pages + 1):
        try:
            query = {'page': i, 'per_page': 100}
            url_request = url_discogs_api + "/users/" + name_discogs_user + "/collection/folders/0/releases"
            response = requests.get(url_request, params=query)
            jsonResponse = response.json()
            collection_items.append(pd.json_normalize(jsonResponse["releases"]))
        except HTTPError as http_err:
            print(f'HTTP error occurred: {http_err}')
        except Exception as err:
            print(f'Other error occurred: {err}')

    df_collection = pd.concat(collection_items, ignore_index=True)
   
    selected_columns = df_collection.columns[~df_collection.columns.isin([ "basic_information.thumb", "basic_information.cover_image",\
     "basic_information.artists", "basic_information.labels", "basic_information.formats", "basic_information.genres", "basic_information.styles"])]
    df_collection = df_collection[selected_columns]

    return(df_collection)

def store_collection_items(df_collection, db_file):

    db = sqlite3.connect(db_file)
    df_collection.to_sql(name="collection", con=db, if_exists='replace')
    db.close()

def retrieve_lowest_value(df_collection, url_discogs_api):

    query = {'curr_abbr': 'EUR'}

    collection_items_value = []
    for i in df_collection.index:
        url_request = url_discogs_api + "/marketplace/stats/" + str(df_collection['id'][i])
        try:
            response = requests.get(url_request, params=query)
            response.raise_for_status()

            df_item = pd.json_normalize(response.json())
            df_item['id'] = str(df_collection['id'][i])
            df_item['time_value_retrieved'] = dt.datetime.now()
            df_item = df_item.loc[:, df_item.columns != 'lowest_price']
            collection_items_value.append(df_item)

        except HTTPError as http_err:
            if response.status_code == 429:
                time.sleep(60)
        except Exception as err:
            print(f'Other error occurred: {err}')
            
    df_collection_value = pd.concat(collection_items_value, ignore_index=True)
    return(df_collection_value)

def store_lowest_value(df_collection_value, db_file):

    db = sqlite3.connect(db_file)
    df_collection_value.to_sql(name="collection_value", con=db, if_exists='append')
    db.close()
