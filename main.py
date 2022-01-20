'''
    What I learned:
        - Have a robust yet agile plan in place first
        - Research how often the API is maintained, to prevent getting stuck halfway through the project
            - If there's lots of bugs on the forums, then maybe don't go with that one.
'''
'''
    Plan:

    def process_data()
        1. Define and import ENVIRONMENT_VARIABLES to access my Spotify account
        2. Encode CLIENT_ID and CLIENT_SECRET into an encoded byte string, then decode into string (for authorization header)
        3. Define the API endpoint (specific song data I'm looking for)
        4. Define headers and data dictionaries, to contain headers and data lists
        5. Define grant type as "client_credentials", because I only care about server connection (no app/client required here)
        6. Make an HTTP POST request to spotify, requesting the OAuth Token (use the provided Token URL)
        7. Store the response JSON object into a variable
        8. 
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

    print ('Yesterday Unix Timestamp: ', yesterday_unix_timestamp_string)

    CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID_ENV')
    CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET_ENV')
    BASE64_ENCODED_HEADER_STRING = base64.b64encode(bytes(f"{CLIENT_ID}:{CLIENT_SECRET}", "ISO-8859-1")).decode("ascii")
    #must be between 1 and 50
    song_limit_amount= 2
    song_limit_amount_string = str(song_limit_amount)

    offset_amount = 5
    offset_amount_string = str(offset_amount)

    print("Song limit amount: ", song_limit_amount_string)

    print('Limit: ' + song_limit_amount_string)

    token_request_url = "https://accounts.spotify.com/api/token"

    headers_dict = {}
    data_dict = {}

    # HTTP GET request returned Status 500, so bug on Spotify's end. Pivoting to other data (1/19/2022)
    # get_request_api_endpoint = f"https://api.spotify.com/v1/me/player/recently-played?limit={song_limit_amount_string}&after={yesterday_unix_timestamp_string}"
    get_request_api_endpoint = f"https://api.spotify.com/v1/users/gw4ojoz76rzzmzy1gchctig37/playlists?offset={offset_amount_string}&limit={song_limit_amount_string}"


    print("get_request_api_endpoint: ", get_request_api_endpoint)

    headers_dict['Authorization'] = f"Basic {BASE64_ENCODED_HEADER_STRING}"
    data_dict['grant_type'] = "client_credentials"
    data_dict['json'] = True

    #multiple scopes requrire a space in the same string
    # data_dict['scope'] = 'user-read-recently-played'
    data_dict['scope'] = 'playlist-read-private'

    # make API request to request OAuth Token
    r = requests.post(token_request_url, data=data_dict, headers=headers_dict)

    token = r.json()['access_token']
    print("Token: " + token)

    # prints the response from the server regarding the access token data
    print(json.dumps(r.json(), indent=2))    

    # use token to access data
    get_request_headers = {
        "Accept": "application/json",
        "Content-Type": 'application/json', #"application/json",
        "Authorization": "Bearer " + token
    }


    # Need to fix how I use the Access Token


    res = requests.get(url=get_request_api_endpoint, headers=get_request_headers)
    
    # recently_played_songs_object = (json.dumps(res.json(), indent=2))
    
    data = (json.dumps(res.json(), indent=2))

    print(data)

    '''
    Thursday 1/20/2022 TODO:
        - Proved that "Recently Played Tracks" endpoint is buggy (Code: 500 - Server Error)
        - Experimented with the "Get User Playlists" endpoint, proved that endpoint is working successfully.
    '''

    '''
        Friday 1/21/2022 TODO:
            1. Investigate Spotify's API endpoints to use
            2. Think through which endpoints could be useful
            3. Narrow useful endpoints down to 1
            4. Rework code to use that endpoint
            5. Create SSIS Package to call Python script
            6. Create scheduled job in SSMS
            7. Set the job to run daily
            8. Check on it Monday 
    '''

    # Bug: https://community.spotify.com/t5/Spotify-for-Developers/Bug-with-an-API-Recently-Played-Tracks/td-p/5065969

    #print(json.dumps(res.json(), indent=2))

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
'''
    # the only field names I care about
    song_names = []
    artist_names = []
    song_added_at_playlist = []
    timestamps = []

    song_dataframe = pd.DataFrame()

    # print(song_dataframe)

    # attempt to append data from API to a dictionary, then to a dataframe
    # try:

        # grabbing elements from data's item list (master results)
    for song in data["items"]:

        # iterating to inner list: names and adding individual name per song
        song_names.append(song["track"]["name"])

        # iterating to individual inner inner list: artists and adding individual artist per son
        artist_names.append(song["track"]["album"]["artists"][0]["name"])

        song_added_at_playlist.append(song["added_at"])

        timestamps.append(song["added_at"][0:10])

    song_dictionary = {
        "Song_Name": song_names,
        "Musician_Name": artist_names,
        "Song_Added_At_Datetime": song_added_at_playlist,
        "Date_Timestamp": timestamps
    }

    song_dataframe = pd.DataFrame(data=song_dictionary, index=False, columns=["Song_Name", "Musician_Name", "Played_At_Datetime", "Date_Timestamp"])

    print(song_dataframe)

    # # Fails if Token is expired
    # except:
    #     print("Token expired. Get a new one.")

    #     # used to prepare pandas dataframe (tabular formatted data)

    # # Validate
    # if check_if_valid_data(song_dataframe) is not False:
    #     print("Data valid, proceed to Load stage.")

    #     # Load
    #     load_dataframe_to_database(song_dataframe, 'KATSUKI', 'API_TESTING')

    # else:
    #     print("Nothing to load. Exiting.")
'''
process_data()
