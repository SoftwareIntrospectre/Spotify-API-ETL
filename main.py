'''
    ETL Program is used to pull played songs data from Spotify API (Extract), validate in Python (Transform), and upload into a SQL Table (Load)

        ToDo (11/12/2021):

            Python
                Programmatically grab the API Token, to avoid "expired token" errors
                    https://towardsdatascience.com/how-to-pull-data-from-an-api-using-python-requests-edcc8d6441b1
                    
                obfuscate UserID and API Token

            SQL
                Determine additional columns
                Enforce datatypes for table
                Create Surrogate Key: Song_Timestamp_Key for TimeStamp Natural Key
                Ensure Surrogate Key is PK in SSMS
                
                Design Schema (Dimension Tables, Fact Tables)
'''

# used for Data Frame functionality (tabular data for easier loading)
import pandas as pd

# create database enging connection
from sqlalchemy import create_engine

# used to open URL for SQL Server server connection
import urllib

# used for Spotify API requests
import requests

# import pip._vendor.requests

# used to convert Unix timestamps into datetime format
from datetime import datetime

import os

import base64

import datetime

# used to make selection from limited options (polymorphic behavior)
from enum import Enum

# import spotipy
# from spotipy.oauth2 import SpotifyClientCredentials

def authenticate():
    '''
        What: Grab the values of environment variables and place them into a dictionary .
        Why: This creates a convenient container for authorization without duplicate logic.
    '''
    CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID_ENV')

    CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET_ENV')

    SPOTIFY_USER_ID = os.environ.get('SPOTIFY_USER_ID_ENV')

    SPOTIFY_URI = 'https://api.spotify.com/v1/me/player/recently-played' #os.environ.get('SPOTIFY_URI')

    encodedData = base64.b64encode(bytes(f"{argument1}:{argument2}", "ISO-8859-1")).decode("ascii")
    BASE64_ENCODED = f"{encodedData}"

    spotify_scopes = 'user-read-recently-played'

    # authenticate
   # sp_oauth = oauth2.SpotifyOauth(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, uri=SPOTIFY_URI, scopes=spotify_scopes)
    # token_info = sp_oauth.get_cached_token()

    # if not token_info:
    #     auth_url = sp_oauth.get_authorize_url(show_dialogue=True)
    #     print(auth_url)
    #     response=input('Paste the above link into your browser, then paste the redirect url here: ')

    #     code = sp_oauth.parse_response_code(response)
    #     token_info = sp_oauth.get_access_token(code)

    #     token = token_info['access_token']

    # sp = spotipy.Spotify(auth=token)
    # print(sp)
   
       # Authorization Code Flow docs: https://developer.spotify.com/documentation/general/guides/authorization/code-flow/
    # Scope docs: https://developer.spotify.com/documentation/general/guides/authorization/scopes/#user-read-recently-played
    get_request_query = "https://accounts.spotify.com/client_id=" + CLIENT_ID, "&","response_type=code&redirect_uri=" + SPOTIFY_URI, "&scope=user-read-recently-played"
    print(get_request_query)
    
    '''
        Why: The token will expire on a timer, and automating this is mandatory for automated ETL jobs.
    '''
    result = ""

    response = requests.post(
    data={"grant type": "authorization_code",
    "refresh_token":  get_request_query},
    headers={"Authorization": "Basic " + response })

    result = response.json()

    # invalid result returns a dictionary with error messages instead of a token
    if result is not str(type):
        #raise Exception("Did not successfully refresh token. Result is: " + str(result))
        return None
    else:
        return result    


# get_refreshed_json_token()

# print(result)


# obtained from Spotify for Developers "Request Token" field
# Token expires after a set period of time. Generate new token as needed.
# URL:    https://developer.spotify.com/console/get-recently-played/?limit=10&after=1636380337&before=

# make sure to click the Relevant Scopes' checkbox, otherwise {'error': {'status': 403, 'message': 'Insufficient client scope'}}

# validation checks
def check_if_valid_data(df: pd.DataFrame) -> bool:

    print(df)

    #check if dataframe is empty
    if df.empty:
        print("No songs played.")
        return False

    # Primary Key (PK) check: played at is PK because it's a unique timestamp (enforces Primary Key Constraint)
    if pd.Series(df['Played_At_Datetime']).is_unique:
        pass

    else:
        raise Exception("Primary Key check is violated.")

    # Null values check
    if df.isnull().values.any():
        raise Exception("Null values found.")

# intended: Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

# testing: Check that all timestamps are of yesterday's date
#     three_days_ago = datetime.datetime.now() - datetime.timedelta(days=3)
#     three_days_ago = three_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)


# comment out if you don't care about the past 24 hours thing

    # timestamps = df["Date_Timestamp"].tolist()
    
    # for timestamp in timestamps:

    #     #strptime creates a datetime object from a given string
    #     if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
    #        raise Exception("At least one of the returned songs does not come from within the last 24 hours.")

    #     # testing: 3 days ago
    #     # if datetime.datetime.strptime(timestamp, "%Y-%m-%d") < three_days_ago:
    #     #     raise Exception("At least one of the returned songs does not come from within the last 72 hours.")

    #return True



# def get_user_recently_played_tracks_current_oauth_token() -> str:
#     '''
#         Function that validates the API token. If not current, request current token and store in environment variable.
#         Use current version of OAuth Token for authentication
#     '''
#     try:
#         oauth_token = env_secrets.SPOTIFY_USER_RECENTLY_PLAYED_TRACKS_TOKEN
#         return oauth_token

#     except:
#         print('Token is not valid. Please refresh the OAuth token.')
#         return None

def set_is_unique() -> bool:

    return True


def establish_sql_server_connection(dataframe_for_sql, server_name, database_name):

    quoted = urllib.parse.quote_plus("Driver={SQL Server};"
                                    "Server=" + server_name + ";" +
                                    "Database=" + database_name +";")

    print('Database connection defined')

    connection_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
    print('Database connection established.')

    dataframe_for_sql.to_sql('Spotify_Data_Load', schema='dbo', con=connection_engine, if_exists='append')
    print('table populated.')

    return True

if __name__ == "__main__":

    oauth_token = os.environ.get('SPOTIFY_OAUTH_TOKEN_ENV')

    # filling in the contents of Spotify API's curl GET request with required fields
    # below the "Try It" button:
    # https://developer.spotify.com/console/get-recently-played/?limit=10&after=1636380337&before=
    
    spotify_API_headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=oauth_token)
    }

    today = datetime.datetime.now()

    # want to grab data from last 24 hours

# intended: yesterday's data (no results 11/9/2021)
    yesterday = today - datetime.timedelta(days=1) #days=1

# testing for 3 days ago worth of data
    # three_days_ago = today -datetime.timedelta(days=3)

    # maths: millisecond * 1000 = 1 second

# intended: yesterday's data (no results 11/9/2021)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

# testing for 3 days ago worth of data
    #three_days_ago_unix_timestamp = int(three_days_ago.timestamp()) * 1000

    # found from first header in curl GET request, using dynamic time instead of hard-coded value

# intended: yesterday's data (no results 11/9/2021)
    http_response= requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=10&after={time}".format(time=yesterday_unix_timestamp), headers = spotify_API_headers)

# testing for 3 days ago worth of data
    #http_response = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=10&after={time}".format(time=three_days_ago_unix_timestamp), headers = spotify_API_headers)

    data = http_response.json()

    print(data)

    #print(data)

    # the only field names I care about

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
        establish_sql_server_connection(song_dataframe, 'KATSUKI', 'API_TESTING')

    else:
        print("Nothing to load. Exiting.")
