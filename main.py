'''
    Intention: 
    
    1. To use my Spotify credentials to connect to Spotify API using the Client Credentials Flow (Authorization: OAuth2) 

    2. Obtain my Recently Played Songs Data (equivalent to: song name, musician name, played at (datetime), datetime_timestamp) from the past 24 hours

    3. Transform the imported data into a tabular format so that it can be export to Microsoft SQL Server

    4. Import data into Microsoft SQL Server --> KATSUKI Server --> [API_Testing].[dbo].[Spotify_Data_Load] table

    5. Upon import: Generate a primary key for each record (Song_Played_SKey), using the played_at_datetime field
        Why: Only 1 song can be played at any given moment in time, making it a good candidate

    6. Have the process automatically occur each day
        6A. Create an SSIS package with the "Execute Script" task

    X. If there's no songs played within past 24 hours, have that be an acceptable case (create a log file for this.)
            *consider what type of logging information would be useful
                * Log each major step. If failure, log exact failure. 
                * If no records for that day, log that case

Why:
    1. Demonstrate ability to grab data via an API via Python
    2. Ability to handle edge cases in script
    3. Demonstrate logging
    4. Experience creating an SSIS package with Python script
    5. Experience maintaining a regularly recurring ETL independently
    6. Experience creating and maintaining data in Dimension Table

Example:
    Song_Name	Musician_Name	Played_At_Datetime	        Date_Timestamp	        Song_Played_SKey (PK)
    The Castle	Nobuo Uematsu	2021-12-08T16:12:57.797Z	2021-12-08	            1
'''

# used to access environment variables
import os

# used to encode CLIENT_ID and CLIENT_SECRET into byte-encoded string for authentication
import base64

# used to make an HTTP request to Spotify's server
import requests

# used to create timestamps for data load from API call (within past 24 hours)
import datetime

# used to convert JSON data into tabular format (dataframe) for SQL Server exporting
import pandas as pd

# used to open URL for SQL Server server connection
import urllib

# create database enging connection
from sqlalchemy import create_engine

import spotipy

import json

# # validation checks
# def check_if_valid_data(df: pd.DataFrame) -> bool:

#     print(df)

#     #check if dataframe is empty
#     if df.empty:
#         print("No songs played.")
#         return False

#     # Primary Key (PK) check: played at is PK because it's a unique timestamp (enforces Primary Key Constraint)
#     if pd.Series(df['Played_At_Datetime']).is_unique:
#         pass

#     else:
#         raise Exception("Primary Key check is violated.")

#     # Null values check
#     if df.isnull().values.any():
#         raise Exception("Null values found.")

def generate_log_table_dataframe():
    '''
        Collect logging data, convert to dataframe, push to table
    '''
    return None

def generate_main_table_dataframe():
    '''
        Take API data, convert to dataframe, push to table
    '''
    return None

def load_dataframe_to_database(dataframe_for_sql, server_name, database_name):
    '''
        load dataframe to database
    '''
    
    quoted = urllib.parse.quote_plus("Driver={SQL Server};"
                                    "Server=" + server_name + ";" +
                                    "Database=" + database_name +";")

    print('Database connection defined')

    connection_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    print('Database connection established.')

    dataframe_for_sql.to_sql('Spotify_Data_Load', schema='dbo', con=connection_engine, if_exists='append')
    print('table populated.')

    return True


def process_data():
        #https://developer.spotify.com/console/get-recently-played/?limit=10&after=1636229680000&before=
    '''
        API Reference: Get Recently Played Tracks
        Endpoint: https://api.spotify.com/v1/me/player/recently-played
        HTTP Method: GET
        OAuth: Required

        GET https://api.spotify.com/v1/me/player/recently-played
        limit = 50 (maximum)
        after 1636229680000 (yesterday_unix_timestamp)

        OAuth Token: GetToken --> Required scopes for this endpoint: user-read-recently-played

    '''
    today = datetime.datetime.now()

    # grab data from last 24 hours
    yesterday = today - datetime.timedelta(days=60)  #days=1
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    yesterday_unix_timestamp_string = str(yesterday_unix_timestamp)

    CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID_ENV')
    CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET_ENV')
    BASE64_ENCODED_HEADER_STRING = base64.b64encode(bytes(f"{CLIENT_ID}:{CLIENT_SECRET}", "ISO-8859-1")).decode("ascii")
    #must be between 1 and 50
    song_limit_amount= 1
    song_limit_amount_string = str(song_limit_amount)

    token_request_url = "https://accounts.spotify.com/api/token"
    headers = {}
    data = {}

    api_call_url = f"https://api.spotify.com/v1/me/player/recently-played?limit={song_limit_amount_string}&after={yesterday_unix_timestamp_string}"

    print("api_call_url: ", api_call_url)

    headers['Authorization'] = f"Basic {BASE64_ENCODED_HEADER_STRING}"
    data['grant_type'] = "client_credentials"
    data['scope'] = 'user-read-recently-played'

    # make API request to request OAuth Token
    r = requests.post(token_request_url, headers=headers, data=data)
    token = r.json()['access_token']

    print("OAuth Token is: ", token)

    # use token to access data
    headers = {
        "Authorization": "Bearer " + token
    }

    res = requests.get(url=api_call_url, headers=headers)
    print(json.dumps(res.json(), indent=2))

#curl -X "GET" "https://api.spotify.com/v1/me/player/recently-played?limit=50&after=1636229680000" -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: Bearer BQAiTwBR21RFKg-MJM4onyICyeTUH-Nftg4p9a-9m4gFxGjnLIXg5GuZQ4OHy9PxkwlXOM1uL58D5fCkAKfe0Z3hMw79rKolf8BM7F3HQ7NH_s8ZTTYDHd136IxqISvOlJDPvY8weTCmY8ivBahdQVtqDZz77_DCSNgUx4I0"

    # spotify_API_headers = {
    #     "Accept": "application/json",
    #     "Content-Type": "application/json",
    #     "Authorization": "Bearer {token}".format(token=oauth_token)
    # }

# intended: yesterday's data (no results 11/9/2021)
    
    # http_response= requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=10&after={time}".format(time=yesterday_unix_timestamp), headers = BASE64_ENCODED_HEADER_STRING)

# testing for 3 days ago worth of data
    #http_response = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=10&after={time}".format(time=three_days_ago_unix_timestamp), headers = spotify_API_headers)

    # data = http_response.json()

    # print(data)

    #print(data)

    # the only field names I care about
'''
    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    print(data)

    song_dataframe = pd.DataFrame()

    # attempt to append data from API to a dictionary, then to a dataframe
    try:

        # grabbing elements from data's item list (master results)
        for song in data["items"]:

            # iterating to inner list: names and adding individual name per song
            song_names.append(song["track"]["name"])

            # iterating to individual inner inner list: artists and adding individual artist per son
            artist_names.append(song["track"]["album"]["artists"][0]["name"])

            played_at_list.append(song["played_at"])

            timestamps.append(song["played_at"][0:10])

        song_dictionary = {
            "Song_Name": song_names,
            "Musician_Name": artist_names,
            "Played_At_Datetime": played_at_list,
            "Date_Timestamp": timestamps
        }

        song_dataframe = pd.DataFrame(data=song_dictionary, index=False, columns=["Song_Name", "Musician_Name", "Played_At_Datetime", "Date_Timestamp"])

    # Fails if Token is expired
    except:
        print("Token expired. Get a new one.")

        # used to prepare pandas dataframe (tabular formatted data)

    # Validate
    if check_if_valid_data(song_dataframe) is not False:
        print("Data valid, proceed to Load stage.")

        # Load
        load_dataframe_to_database(song_dataframe, 'KATSUKI', 'API_TESTING')

    else:
        print("Nothing to load. Exiting.")
'''

process_data()
