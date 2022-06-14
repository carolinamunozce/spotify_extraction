"""
Extraction that it allows to get the information of my count of Spotify, show them
what i have listed last 24 hours

The idea is to create the ETL.
"""

#import sqlalchemy
import pandas as pd 
#from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


DATABASE_LOCATION = ""

# Data user SP

USER_ID = "carolina.munozce"
TOKEN = "BQBWr-KU7mV9ZBjdi3FXI2FjEL5_klcUVWUUyZnYtfOcuBL19LyEGJk_F1FUYF2bAF55QoJuNVbipCA05X8SBIsk4_faRkBbiXUHlUV0aL9hicTj5zkuqXZlH-Kxk6tPotm5Nw8XQT3lYkECIpNIZIcK2mZs"

def check_if_valid_data(df: pd.DataFrame) -> bool:
   #Check if dataframe is empty. In this case, if we don't listen any music on SP
   if df.empty:
       print(" No songs download. Finish execution")
       return False

    # Primary key check -> For duplicate data
   if pd.Series(df['played_at']).is_unique:
        pass
   else:
       raise Exception("Primary key check is violed")

    # Check any null
   #if df.isnull().values.any():
   #     raise Exception("Null value found")

    # Check that all timestamps are of yesterday's date
   yesterday  = datetime.datetime.now() - datetime.timedelta(days=1)
   yesterday = yesterday.replace(hour=0, minute=0, second=0,microsecond=0)

   timestamps = df["timestamps"].tolist()
   for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp,"%Y-%m-%d") != yesterday:
            raise Exception("At least one of the returned songs does not come fron within the last 24 hours")
   return True

if __name__ == "__main__":
    
   headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }
    #Convert unix timestamp in milliseconds
   today =datetime.datetime.now()
   yesterday = today - datetime.timedelta(days=1)
   yesterday_unix_timestamp = int(yesterday.timestamp())* 1000

    # Get request from SP last 24 hours

   r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers = headers)

   data = r.json()
   # print (data)

   song_names = []
   artist_names = []
   played_at_list = []
   timestamps = []

   for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

   song_dict = {
        "song_name": song_names,
        "artist_names": artist_names,
        "played_at": played_at_list,
        "timestamps": timestamps
    }

   song_df = pd.DataFrame(song_dict, columns= ["song_name","artist_name","played_at","timestamps"])
    
    #Validate the song
   if check_if_valid_data(song_df):
        print("Data valid, process to Load stage")

   print (song_df)

    