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

# used to format JSON data
import json

# def generate_log_table_dataframe():
#     '''
#         Collect logging data, convert to dataframe, push to table
#     '''
#     return None

# def generate_main_table_dataframe():
#     '''
#         Take API data, convert to dataframe, push to table
#     '''
#     return None

def load_dataframe_to_database(dataframe_for_sql, server_name, database_name, schema_name, table_name):

    quoted = urllib.parse.quote_plus("Driver=SQL Server;"
                                    "Server=" + server_name + ";" +
                                    "Database=" + database_name +";")

    print('Database connection defined')

    connection_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    print('Database connection established.')

    dataframe_for_sql.to_sql(table_name, schema=schema_name, con=connection_engine, if_exists='append', index=False)#, dtype=dict)
    print('table populated.')

    return True


def process_data():
        #https://developer.spotify.com/console/get-recently-played/?limit=10&after=1636229680000&before=
    '''
        API Reference: Get New Releases
        Endpoint: https://api.spotify.com/v1/browse/new-releases
        HTTP Method: GET
        OAuth: Required
        https://developer.spotify.com/console/get-new-releases/?country=US&limit=10&offset=5

        limit = 10  (maximum)
        offset = 5
        after 1636229680000 (yesterday_unix_timestamp)

        OAuth Token: GetToken --> Required scopes for this endpoint: user-read-recently-played

    '''
    today = datetime.datetime.now()

    # grab data from last 24 hours
    yesterday = today - datetime.timedelta(days=60)  #days=1
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    yesterday_unix_timestamp_string = str(yesterday_unix_timestamp)

    print ('Yesterday Unix Timestamp: ', yesterday_unix_timestamp_string)

    CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID_ENV')
    CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET_ENV')
    BASE64_ENCODED_HEADER_STRING = base64.b64encode(bytes(f"{CLIENT_ID}:{CLIENT_SECRET}", "ISO-8859-1")).decode("ascii")
    #must be between 1 and 50
    song_limit_amount= 10
    song_limit_amount_string = str(song_limit_amount)

    print('Limit: ' + song_limit_amount_string)

    token_request_url = "https://accounts.spotify.com/api/token"
    headers = {}
    data = {}

    #api_call_url = f"https://api.spotify.com/v1/me/player/recently-played?limit={song_limit_amount_string}&after={yesterday_unix_timestamp_string}"
    api_endpoint_url  = "https://api.spotify.com/v1/browse/new-releases?country=US&limit=10&offset=5"
    print("api_call_url: ", api_endpoint_url)

    headers['Authorization'] = f"Basic {BASE64_ENCODED_HEADER_STRING}"
    data['grant_type'] = "client_credentials"
    data['json'] = True

    #multiple scopes requrire a space in the same string
    #data['scope'] = 'user-top-read'

    # make API request to request OAuth Token
    r = requests.post(token_request_url, headers=headers, data=data)

    # prints the response from the server regarding the access token data
    print(json.dumps(r.json(), indent=2))
    
    token = r.json()['access_token']

    # use token to access data
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer " + token
    }

    res = requests.get(url=api_endpoint_url, headers=headers)

    print(res)    
    #data = json.dumps(res.json(), indent=2)
    data = res.json()

    print(data)

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

    # print(data)

    #print(data)

    output_dataframe = pd.DataFrame()

# the only field names I care about
    album_ids_list = [] #"id": "0sb3OPjnOZEgWrAhwSyGJc"
    album_types_list  = [] #"type": "album"
    album_names_list  = [] #"name": "on to better things",
    album_release_dates_list  = [] # "release_date": "2022-01-21"
    album_release_dates_precision_list = [] # "release_date_precision" : "day"
    album_total_tracks_list  = [] #"total_tracks": 15
    album_spotify_urls_list  = [] #"external_urls": {"spotify": "https://open.spotify.com/album/0sb3OPjnOZEgWrAhwSyGJc"}
    album_artist_ids_list  = [] #"id": "6ASri4ePR7RlsvIQgWPJpS"
    album_artist_names_list  = [] #"name": "iaan dior"

# album_dictionary:  {
#     'Album_ID': ['0sb3OPjnOZEgWrAhwSyGJc']
#     , 'Album_Type': ['album']
#     , 'Album_Name': ['on to better things']
# , 'Album_Release_Date': ['2022-01-21']
# , 'Album_Release_Date_Precision': ['day']
# , 'Album_Total_Tracks': [15]
# , 'Album_Spotify_URL': ['https://open.spotify.com/album/0sb3OPjnOZEgWrAhwSyGJc']
# , 'Album_Artist_ID': ['6ASri4ePR7RlsvIQgWPJpS']
# , 'Album_Artist_Name': ['iann dior']
# }

    '''
        "album_type": "album",
        "artists": [
            "id": "6ASri4ePR7RlsvIQgWPJpS"
            "name": "iann dior"
        ]
        "id": "0sb3OPjnOZEgWrAhwSyGJc"
    '''

    # song_dataframe = pd.DataFrame()

    # attempt to append data from API to a dictionary, then to a dataframe

    # grabbing elements from data's item list (master results)
    for item in data["albums"]["items"]:
    
        print("album_id: ", item["id"])
        album_ids_list.append(item["id"])

        print("album_type: ", item["album_type"])
        album_types_list.append(item["album_type"])

        print("album_name: ", item["name"])
        album_names_list.append(item["name"])

        print("release_date: ", item["release_date"])
        album_release_dates_list.append(item["release_date"])

        print("release_date_precision: ", item["release_date_precision"])
        album_release_dates_precision_list.append(item["release_date_precision"])

        print("total_tracks: ", item["total_tracks"])
        album_total_tracks_list.append(item["total_tracks"])
        
        print("spotify_url: ", item["external_urls"]["spotify"])
        album_spotify_urls_list.append(item["external_urls"]["spotify"])

        for artist in item["artists"]:
            print("artist_id: ", artist["id"])
            album_artist_ids_list.append(artist["id"])

            print("artist_name: ", artist["name"])
            album_artist_names_list.append(artist["name"])


        album_dictionary = {
            "Album_ID": album_ids_list,
            "Album_Type": album_types_list,
            "Album_Name": album_names_list,
            "Album_Release_Date": album_release_dates_list,
            "Album_Release_Date_Precision": album_release_dates_precision_list,
            "Album_Total_Tracks": album_total_tracks_list,
            "Album_Spotify_URL": album_spotify_urls_list,
            # "Album_Artist_ID": album_artist_ids_list,
            # "Album_Artist_Name": album_artist_names_list
            }

        print("album_dictionary: ", album_dictionary)

        dataframe_columns = {
            "Album_ID": album_dictionary["Album_ID"],
            "Album_Type": album_dictionary["Album_Type"],
            "Album_Name": album_dictionary["Album_Name"],
            "Album_Release_Date": album_dictionary["Album_Release_Date"],
            "Album_Release_Date_Precision": album_dictionary["Album_Release_Date_Precision"],
            "Album_Total_Tracks": album_dictionary["Album_Total_Tracks"],
            "Album_Spotify_URL": album_dictionary["Album_Spotify_URL"],
            # "Album_Artist_ID": album_dictionary["Album_Artist_ID"],   #TODO: Fix the One to Many problem (multiple artists per album potentially). Maybe use a Lambda Function here?
            # "Album_Artist_Name": album_dictionary["Album_Artist_Name"] #TODO: Fix the One to Many problem (multiple artists per album potentially). Maybe use a Lambda Function here?
        }

        album_dataframe = pd.DataFrame(dataframe_columns) 
        print("Album Dataframe: ",album_dataframe)
        

        # concatenates each album's dataframe to the output dataframe
        print("Appending current dataframe to Output Dataframe")
        output_dataframe = output_dataframe.append(album_dataframe)

        print(output_dataframe)
    #     return

        output_dataframe = output_dataframe.drop_duplicates()

        print(output_dataframe)

    print('Finished iterating through JSON data. Moving to SQL Load.')
    load_dataframe_to_database(output_dataframe, 'KATSUKI', 'API_TESTING', 'dbo', 'Spotify_Top_Ten_New_Releases') 
    
    #TODO: Fix the issue loading this into SQL: "(pyodbc.Programming Error"
        # Exception has occurred: ProgrammingError
        # (pyodbc.ProgrammingError) ('42000', "[42000] [Microsoft][ODBC SQL Server Driver][SQL Server]Incorrect syntax near ')'. (102) (SQLExecDirectW)")
        # [SQL: 
        # CREATE TABLE dbo.[Spotify_Top_Ten_New_Releases] (
        # )

        # ]

process_data()
