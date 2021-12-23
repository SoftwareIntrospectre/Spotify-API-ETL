'''
    ETL Program is used to pull played songs data from Spotify API (Extract), validate in Python (Transform), and upload into a SQL Table (Load)

        # ToDo (11/12/2021):

            # Python
                # Programmatically grab the API Token, to avoid "expired token" errors
                    # https://towardsdatascience.com/how-to-pull-data-from-an-api-using-python-requests-edcc8d6441b1
                    
                # obfuscate UserID and API Token

            # SQL
                # Determine additional columns
                # Enforce datatypes for table
                # Create Surrogate Key: Song_Timestamp_Key for TimeStamp Natural Key
                # Ensure Surrogate Key is PK in SSMS
                
                # Design Schema (Dimension Tables, Fact Tables)
'''

# used for Data Frame functionality (tabular data for easier loading)
# import pandas as pd

# # create database enging connection
# from sqlalchemy import create_engine

# # used to open URL for SQL Server server connection
# import urllib

# # used for Spotify API requests
# import requests

import pip._vendor.requests

# used to convert Unix timestamps into datetime format
from datetime import datetime

import os

import base64

import datetime

#used to make selection from limited options (polymorphic behavior)
from enum import Enum

# used to create expansive list of cases if needed
secrets_options_dictionary = {
     "USER_ID": 1
    , "TOKEN": 2
    , "CLIENT_ID": 3 
    , "CLIENT_SECRET": 4
    , "URI": 5
}
        
def base64_encode(argument1, **argument2):
    '''
        What: Encodes the bytes the combined client_id and client_secret strings, then decodes them into a string.
        Why: This is necessary for authorization with Spotify REST API. Allows for 1 or 2 arguments for this.
    '''

    # allowing an optional second argument
    two_arguments = False

    if argument2 is not None:
        two_arguments = True

    match two_arguments:

        # when both arguments are provided
        case False:
            encodedData = base64.b64encode(bytes(f"{argument1}", "ISO-8859-1")).decode("ascii")
            authorization_header_string = f"{encodedData}"
            return(authorization_header_string)

        # when only 1 argument is provided
        case True:
            encodedData = base64.b64encode(bytes(f"{argument1}:{argument2}", "ISO-8859-1")).decode("ascii")
            authorization_header_string = f"{encodedData}"
            return(authorization_header_string)

        case __:
            raise Exception("Nothing provided to encode. Please provide at least 1 string to encode.")

def get_sensitive_item(option_keyword):
    '''
        What: Grabs the value of an environment variable based on the limited-options input.
        Why: Serves to minimize the amount of duplicated logic for similar operations (polymorphic without classes too).
    '''
    valid_secret = ""

    # uses case statements to provide polymorphic behavior based on input
    # Python 3.10 allows for "match-case" syntax
        # Article: https://www.pythonpool.com/match-case-python/
    match option_keyword:

        # change each case to whatever specific environment variable and location you need
        case 1:
            valid_secret = os.environ.get('SPOTIFY_USER_ID_ENV')
            # print('USER_ID: ' + valid_secret)
            return valid_secret

        case 2:
            valid_secret = os.environ.get('SPOTIFY_USER_RECENTLY_PLAYED_TRACKS_TOKEN_ENV')
            # print('TOKEN: ' + valid_secret)
            return valid_secret

        case 3:
            valid_secret =  os.environ.get('SPOTIFY_CLIENT_ID_ENV')
            # print('CLIENT_ID: ' + valid_secret)
            return valid_secret

        case 4:
            valid_secret = os.environ.get('SPOTIFY_CLIENT_SECRET_ENV')
            # print('CLIENT_SECRET: ' + valid_secret)
            return valid_secret

        case 5:
            valid_secret = os.environ.get('SPOTIFY_URI')
            # print('CLIENT_SECRET: ' + valid_secret)
            return valid_secret

        # default case: not valid
        case _:
            raise Exception("Not a valid keyword. Please enter a valid option from this list: " + str(secrets_options_dictionary))

# convenient way to retrieve sensitive data 
def get_authorization_credentials_dictionary():
    '''
        What: Grab the values of environment variables and place them into a dictionary .
        Why: This creates a convenient container for authorization without duplicate logic.
    '''

    # immutable data type: only want to provide limited options

    CLIENT_ID = get_sensitive_item(secrets_options_dictionary['CLIENT_ID'])

    CLIENT_SECRET = get_sensitive_item(secrets_options_dictionary["CLIENT_SECRET"])

    SPOTIFY_USER_ID = get_sensitive_item(secrets_options_dictionary["USER_ID"])

    SPOTIFY_URI = get_sensitive_item(secrets_options_dictionary["URI"])

    environment_variables_dictionary = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'user_id': SPOTIFY_USER_ID, 
        'uri': SPOTIFY_URI
    }

    #return get_refreshed_json_token(environment_variables_dictionary)
    # Authorization Code Flow docs: https://developer.spotify.com/documentation/general/guides/authorization/code-flow/
    # Scope docs: https://developer.spotify.com/documentation/general/guides/authorization/scopes/#user-read-recently-played
    post_request_query = "https://accounts.spotify.com/client_id=" + str(environment_variables_dictionary["client_id"]), "&","response_type=code&redirect_uri=" + str(base64_encode(environment_variables_dictionary["uri"])) , "&scope=user-read-recently-played"
    print(post_request_query)

    return get_refreshed_json_token(post_request_query, environment_variables_dictionary)

def get_refreshed_json_token(query, secrets_dictionary):

    print(query)

    print(secrets_dictionary)

    # print(secrets_options_dictionary)
    
    # resume this: https://youtu.be/-FsFT6OwE1A?t=166
    
    '''
        Why: The token will expire on a timer, and automating this is mandatory for automated ETL jobs.
    '''
    result = ""


    # https://www.youtube.com/watch?v=-FsFT6OwE1A&t=166s
    response = pip._vendor.requests.post(query,
    data={"grant type": "refresh_token",
    "refresh_token": token_to_refresh},
    headers={"Authorization": "Basic " +  })

    result = response.json()

    # invalid result returns a dictionary with error messages instead of a token
    if result is not str(type):
        #raise Exception("Did not successfully refresh token. Result is: " + str(result))
        return None
    else:
        return result    


get_refreshed_json_token()

# result = get_authorization_credentials_dictionary()

# print(result)


# obtained from Spotify for Developers "Request Token" field
# Token expires after a set period of time. Generate new token as needed.
# URL:    https://developer.spotify.com/console/get-recently-played/?limit=10&after=1636380337&before=

# make sure to click the Relevant Scopes' checkbox, otherwise {'error': {'status': 403, 'message': 'Insufficient client scope'}}

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

# # intended: Check that all timestamps are of yesterday's date
#     yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
#     yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

# # testing: Check that all timestamps are of yesterday's date
# #     three_days_ago = datetime.datetime.now() - datetime.timedelta(days=3)
# #     three_days_ago = three_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)


# # comment out if you don't care about the past 24 hours thing

#     # timestamps = df["Date_Timestamp"].tolist()
    
#     # for timestamp in timestamps:

#     #     #strptime creates a datetime object from a given string
#     #     if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
#     #        raise Exception("At least one of the returned songs does not come from within the last 24 hours.")

#     #     # testing: 3 days ago
#     #     # if datetime.datetime.strptime(timestamp, "%Y-%m-%d") < three_days_ago:
#     #     #     raise Exception("At least one of the returned songs does not come from within the last 72 hours.")

#     return True



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

# def set_is_unique() -> bool:

#     return True


# def establish_sql_server_connection(dataframe_for_sql, server_name, database_name):

#     quoted = urllib.parse.quote_plus("Driver={SQL Server};"
#                                     "Server=" + server_name + ";" +
#                                     "Database=" + database_name +";")

#     print('Database connection defined')

#     connection_engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(quoted))
#     print('Database connection established.')

#     dataframe_for_sql.to_sql('Spotify_Data_Load', schema='dbo', con=connection_engine, if_exists='append')
#     print('table populated.')

#     return True


# if __name__ == "__main__":

#     oauth_token = get_user_recently_played_tracks_current_oauth_token()

#     # filling in the contents of Spotify API's curl GET request with required fields
#     # below the "Try It" button:
#     # https://developer.spotify.com/console/get-recently-played/?limit=10&after=1636380337&before=
    
#     spotify_API_headers = {
#         "Accept": "application/json",
#         "Content-Type": "application/json",
#         "Authorization": "Bearer {token}".format(token=oauth_token)
#     }

#     today = datetime.datetime.now()

#     # want to grab data from last 24 hours

# # intended: yesterday's data (no results 11/9/2021)
#     yesterday = today - datetime.timedelta(days=30) #days=1

# # testing for 3 days ago worth of data
#     # three_days_ago = today -datetime.timedelta(days=3)

#     # maths: millisecond * 1000 = 1 second

# # intended: yesterday's data (no results 11/9/2021)
#     yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

# # testing for 3 days ago worth of data
#     #three_days_ago_unix_timestamp = int(three_days_ago.timestamp()) * 1000

#     # found from first header in curl GET request, using dynamic time instead of hard-coded value

# # intended: yesterday's data (no results 11/9/2021)
#     http_response= requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=10&after={time}".format(time=yesterday_unix_timestamp), headers = spotify_API_headers)

# # testing for 3 days ago worth of data
#     #http_response = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=10&after={time}".format(time=three_days_ago_unix_timestamp), headers = spotify_API_headers)

#     data = http_response.json()

#     print(data)

#     #print(data)

#     # the only field names I care about

#     song_names = []
#     artist_names = []
#     played_at_list = []
#     timestamps = []

#     print(data)

#     song_dataframe = pd.DataFrame()

#     # attempt to append data from API to a dictionary, then to a dataframe
#     try:

#         # grabbing elements from data's item list (master results)
#         for song in data["items"]:

#             # iterating to inner list: names and adding individual name per song
#             song_names.append(song["track"]["name"])

#             # iterating to individual inner inner list: artists and adding individual artist per son
#             artist_names.append(song["track"]["album"]["artists"][0]["name"])

#             played_at_list.append(song["played_at"])

#             timestamps.append(song["played_at"][0:10])

#         song_dictionary = {
#             "Song_Name": song_names,
#             "Musician_Name": artist_names,
#             "Played_At_Datetime": played_at_list,
#             "Date_Timestamp": timestamps
#         }

#         song_dataframe = pd.DataFrame(song_dictionary, columns = ["Song_Name", "Musician_Name", "Played_At_Datetime", "Date_Timestamp"])

#     # Fails if Token is expired
#     except:
#         print("Token expired. Get a new one.")

#         # used to prepare pandas dataframe (tabular formatted data)


#     # Validate
#     if check_if_valid_data(song_dataframe) is not False:
#         print("Data valid, proceed to Load stage.")

#         # Load
#         establish_sql_server_connection(song_dataframe, 'KATSUKI', 'API_TESTING')

#     else:
#         print("Nothing to load. Exiting.")



