from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import isodate
import uuid
from datetime import datetime
import streamlit as st
import warnings
warnings.filterwarnings("ignore", category=UserWarning, message="missing ScriptRunContext!")

#api_key = "AIzaSyC7w9y8ZvSrhD06-BNqVGTa_xzwLdt0Pzg"
#api_key = "AIzaSyC7w9y8ZvSrhD06-BNqVGTa_xzwLdt0Pzg"
api_key = "AIzaSyAMW1l2tFN7r00v56cS0WLL6_uEHKCoPVs"
youtube = build('youtube', 'v3', developerKey=api_key)

#get channel id
def get_channel_info(channel_id):

    response = youtube.channels().list(
        id=channel_id,
        part='snippet,statistics,contentDetails'
    ).execute()

    for i in response['items']:
        data=dict(Channel_Names=i['snippet']['title'],
                  channel_Id=i['id'],
                  subscribers=i['statistics']['subscriberCount'],
                  Views=i['statistics']['viewCount'],
                  Total_Videos=i['statistics']['videoCount'],
                  Chennal_Description=i['snippet']['description'],
                  Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
                  )
    return data


# get video ids
from googleapiclient.errors import HttpError

def get_videos_ids(channel_id):
    video_ids = []
    
    # Fetch the upload playlist ID for the channel
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    # Loop to retrieve video IDs from the upload playlist
    while True:
        response1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        for item in response1['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])

        next_page_token = response1.get('nextPageToken')

        if not next_page_token:
            break

    return video_ids



#GET VIDEO INFORMATION
def get_video_info(video_ids):
  video_data=[]
  try:
      for video_id in video_ids:

          response = youtube.videos().list(part='snippet,ContentDetails,statistics',
                                        id=video_id).execute()
          for i in response['items']:
            data=dict(Channel_Names=i['snippet']['channelTitle'],
                      channel_Id=i['snippet']['channelId'],
                      Video_Id=i['id'],
                      Title=i['snippet']['title'],
                      Tags=i.get('tags'),
                      Thumbnail=i['snippet']['thumbnails'],
                      Description=i.get('description'),
                      Published_Date=i['snippet']['publishedAt'],
                      Duration=i['contentDetails']['duration'],
                      views=i.get('viewCount'),
                      Comments=i.get('commentCount'),
                      Favorite_Count=i['statistics']['favoriteCount'],
                      Definition=i['contentDetails']['definition'],
                      Caption_Status=i['contentDetails']['caption'])
            video_data.append(data)
  except:
        pass
  return video_data

#get comment info
def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            response = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
                key=api_key
            ).execute()

            for item in response.get('items', []):
                snippet = item.get('snippet', {}).get('topLevelComment', {}).get('snippet', {})
                data = {
                    'comment_Id': snippet.get('id'),
                    'Video_id': snippet.get('videoId'),
                    'Comment_Text': snippet.get('textDisplay'),
                    'Comment_Author': snippet.get('authorDisplayName'),
                    'Comment_Published': snippet.get('publishedAt')
                }
                comment_data.append(data)

    except Exception as e:
        print("An error occurred:", str(e))

    return comment_data


#get playlist_details
def get_playlist_details(channal_id):
  All_data=[]
  next_page_token=None
  while True:
    response=youtube.playlists().list(
        part='snippet,contentDetails',
        channelId=channal_id,
        maxResults=50,
        pageToken=next_page_token).execute()
    for i in response['items']:
      data=dict(Playlist_Id=i['id'],
              Title=i['snippet']['title'],
                Channel_Id=i['snippet']['channelId'],
                Channel_Name=i['snippet']['channelTitle'],
                PublishedAt=i['snippet']['publishedAt'],
                Video_Count=i['contentDetails']['itemCount'])
      All_data.append(data)
    next_page_token=response.get('nextPageToken')
    if next_page_token is None:
     break

  return All_data

  

def channel_details(channel_id):
  channel_details=get_channel_info(channel_id)
  playlist_details=get_playlist_details(channel_id)
  video_ids=get_videos_ids(channel_id)
  video_details=get_video_info(video_ids)
  comment_details=get_comment_info(video_ids)

#insert=channel_details('UCs0zfLJmh564g_f1qDQIT0g')
#insert

import pymongo

client=pymongo.MongoClient("mongodb://subbu:vembu@ac-hxu0gpw-shard-00-00.udikzhc.mongodb.net:27017,ac-hxu0gpw-shard-00-01.udikzhc.mongodb.net:27017,ac-hxu0gpw-shard-00-02.udikzhc.mongodb.net:27017/?ssl=true&replicaSet=atlas-bvhqgr-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Connect to the MongoDB database
db = client["Youtube_data"]

def channel_details(channel_id):
    try:
        logging.info(f"Fetching details for channel ID: {channel_id}")
        
        # Gather channel, playlist, video, and comment details
        channel_info = get_channel_info(channel_id)
        playlist_info = get_playlist_details(channel_id)
        video_ids = get_videos_ids(channel_id)
        video_info = get_video_info(video_ids)
        comment_info = get_comment_info(video_ids)

        # Prepare data for insertion
        data_to_insert = {
            "channel_information": channel_info,
            "playlist_information": playlist_info,
            "video_information": video_info,
            "comment_information": comment_info
        }
        
        # Insert data into the MongoDB collection
        collection = db["channel_details"]
        collection.insert_one(data_to_insert)
        
        logging.info("Data uploaded successfully.")
        return "Upload successful"
    
    except Exception as e:
        logging.error(f"Error occurred while processing channel ID {channel_id}: {e}")
        return f"An error occurred: {e}"

# Example of using the function
# channel_id = 'UCs0zfLJmh564g_f1qDQIT0g'  # Replace with your actual channel ID
# insert_result = channel_details(channel_id)
# print(insert_result)

#insert

db=client["Youtube_data"]
coll1= db["channel_details"]



#def channels_table():
# channel table
import mysql.connector
import pandas as pd
from pymongo import MongoClient

def channels_table(channel_name):
    # Connect to MySQL
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='youtube'
    )
    cursor = mydb.cursor()

    # Drop the table if it exists
    drop_query = '''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create the channels table if it doesn't exist
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS channels(
                            Channel_Names VARCHAR(100),
                            channel_Id VARCHAR(80) PRIMARY KEY,
                            subscribers BIGINT,
                            Views BIGINT,
                            Total_Videos INT,
                            Channel_Description TEXT,
                            Playlist_Id VARCHAR(80)
                        )'''
        cursor.execute(create_query)
        mydb.commit()
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

   

    # Fetch channel information from MongoDB
    single_channel_details = []
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        if "channel_information" in ch_data:
            single_channel_details.append(ch_data["channel_information"])

    # Check if there's data to process
    if single_channel_details:
        df_single_channel = pd.DataFrame(single_channel_details)
    else:
        print("No channel data found in MongoDB.")
        return  # Exit if no data found

    # Insert data into MySQL
    insert_query = '''INSERT INTO channels(
                        Channel_Names,
                        channel_Id,
                        subscribers,
                        Views,
                        Total_Videos,
                        Channel_Description,
                        Playlist_Id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''

    for index, row in df_single_channel.iterrows():
        values = (
            row.get('Channel_Names'),
            row.get('channel_Id'),
            row.get('subscribers'),
            row.get('Views'),
            row.get('Total_Videos'),
            row.get('Channel_Description'),
            row.get('Playlist_Id')
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.IntegrityError as ie:
            print(f"IntegrityError: Record with channel_Id {row['channel_Id']} already exists. Skipping.")
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")
            mydb.rollback()

    # Close the cursor and connection
    cursor.close()
    mydb.close()
    print("MySQL connection closed.")

# Call the function
channels_table("channel_name")  # Replace with an actual channel name if needed



# Comment table
#comment table
import mysql.connector
import pandas as pd
from pymongo import MongoClient
import uuid
import isodate
import json

def sanitize_utf8(text):
    if isinstance(text, str):
        return text.encode('utf-8', 'ignore').decode('utf-8')
    return text

def comments_table(channel_name):
   

    # Connect to MySQL with utf8mb4 charset to support all Unicode characters (including emojis)
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='root',
        database='youtube',
        charset='utf8mb4'  # Ensure connection uses utf8mb4 character set
    )
    cursor = mydb.cursor()

    # Drop the table if it exists
    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    mydb.commit()

    # Create the comments table if it doesn't exist
    create_query = '''CREATE TABLE IF NOT EXISTS comments(
                        comment_Id VARCHAR(100) PRIMARY KEY,
                        Video_id VARCHAR(50),
                        Comment_Text TEXT CHARACTER SET utf8mb4,  #Explicitly set charset for Comment_Text
                        Comment_Author VARCHAR(150) CHARACTER SET utf8mb4, 
                        Comment_Published TIMESTAMP
                    )'''
    cursor.execute(create_query)
    mydb.commit()

    # Fetch comment information from MongoDB
    single_channel_details = []
    for ch_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        if "comment_information" in ch_data:
            single_channel_details.extend(ch_data["comment_information"])
    df_single_channel = pd.DataFrame(single_channel_details)
   
    # Insert DataFrame rows into the MySQL table
    insert_query = '''INSERT INTO comments(
                        comment_Id,
                        Video_id,
                        Comment_Text,
                        Comment_Author,
                        Comment_Published
                    ) VALUES (%s, %s, %s, %s, %s)'''

    for index, row in df_single_channel.iterrows():
        # If comment_Id is None, generate a unique UUID
        comment_id = row.get('comment_Id') or str(uuid.uuid4())
        
        # Convert ISO 8601 timestamp to MySQL-compatible format (if not already in correct format)
        try:
            comment_published = isodate.parse_datetime(row.get('Comment_Published')).strftime('%Y-%m-%d %H:%M:%S') if row.get('Comment_Published') else None
        except (ValueError, TypeError):
            print(f"Skipping invalid datetime value for comment_Id {comment_id}")
            continue

        # Sanitize Comment_Text
        comment_text = sanitize_utf8(row.get('Comment_Text', ''))

        values = (
            comment_id,
            row.get('Video_id'),
            comment_text,  # Ensure Comment_Text is properly encoded
            row.get('Comment_Author'),
            comment_published  # Use the converted datetime value
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.IntegrityError as ie:
            print(f"IntegrityError: Record with comment_Id {comment_id} already exists. Skipping.")
        except mysql.connector.DataError as de:
            print(f"DataError: {de}")
            print(f"Problematic data: {values}")
            mydb.rollback()
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")
            mydb.rollback()

    # Close the cursor and connection
    cursor.close()
    mydb.close()
    print("MySQL connection closed.")

# Call the function
comments_table("channel_name")


import mysql.connector
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import isodate
import json

def videos_table(channel_name):
    # Function to convert datetime from ISO 8601 to MySQL format
    def convert_to_mysql_datetime(iso_datetime):
        try:
            return datetime.strptime(iso_datetime, "%Y-%m-%dT%H:%M:%SZ").strftime('%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None  # Return None if conversion fails

    # Function to convert duration from ISO 8601 to MySQL TIME format
    def convert_to_mysql_time(iso_duration):
        try:
            duration = isodate.parse_duration(iso_duration)
            if isinstance(duration, timedelta):
                total_seconds = int(duration.total_seconds())
                hours = total_seconds // 3600
                minutes = (total_seconds % 3600) // 60
                seconds = total_seconds % 60
                return f'{hours:02}:{minutes:02}:{seconds:02}'
        except (isodate.ISO8601Error, TypeError):
            return None  # Return None if the format is invalid

    # Connect to MySQL
    print("Connecting to MySQL database...")
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='root',
        database='youtube',
        charset='utf8mb4'
    )
    cursor = mydb.cursor()
    print("Connected to MySQL database.")

    # Drop the table if it exists (optional)
    try:
        drop_query = '''DROP TABLE IF EXISTS videos'''
        cursor.execute(drop_query)
        mydb.commit()
        print("Dropped existing videos table (if it existed).")
    except Exception as e:
        print(f"Error dropping table: {e}")

    # Create the table if it does not exist
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS videos (
                            Channel_Names VARCHAR(255) CHARACTER SET utf8mb4,
                            channel_Id VARCHAR(255) CHARACTER SET utf8mb4,
                            Video_Id VARCHAR(255) PRIMARY KEY,
                            Title VARCHAR(255) CHARACTER SET utf8mb4,
                            Tags TEXT CHARACTER SET utf8mb4,
                            Thumbnail VARCHAR(1000) CHARACTER SET utf8mb4,
                            Description TEXT CHARACTER SET utf8mb4,
                            Published_Date DATETIME NULL,
                            Duration TIME NULL,
                            Views INT,
                            Comments INT,
                            Favorite_Count INT,
                            Definition VARCHAR(50) CHARACTER SET utf8mb4,
                            Caption_Status VARCHAR(50) CHARACTER SET utf8mb4
                        ) CHARACTER SET utf8mb4;'''
        
        cursor.execute(create_query)
        mydb.commit()
        print("Videos table created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

    # Fetch video information from MongoDB
    video_data_list = []  # List to hold all videos
    for ch_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        if "video_information" in ch_data:
            video_data_list.extend(ch_data["video_information"])

    # Convert the fetched data into a DataFrame
    df_single_channel = pd.DataFrame(video_data_list)

    # Convert datetime values to MySQL-compatible format
    if 'Published_Date' in df_single_channel.columns:
        df_single_channel['Published_Date'] = df_single_channel['Published_Date'].apply(
            lambda x: convert_to_mysql_datetime(x) if isinstance(x, str) else None
        )

    # Convert duration values to MySQL-compatible TIME format
    if 'Duration' in df_single_channel.columns:
        df_single_channel['Duration'] = df_single_channel['Duration'].apply(
            lambda x: convert_to_mysql_time(x) if isinstance(x, str) else None
        )

    # Handle dictionary types (convert to URLs or JSON string)
    def handle_dict(value):
        if isinstance(value, dict):
            if 'default' in value and 'url' in value['default']:
                return value['default']['url']
            else:
                return json.dumps(value)  # Convert the dictionary to a JSON string
        return value  # Return value as is if it's not a dictionary

    # Insert DataFrame rows into the MySQL table
    insert_query = '''INSERT INTO videos(
                        Channel_Names,
                        channel_Id,
                        Video_Id,
                        Title,
                        Tags,
                        Thumbnail,
                        Description,
                        Published_Date,
                        Duration,
                        Views,
                        Comments,
                        Favorite_Count,
                        Definition,
                        Caption_Status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

    for index, row in df_single_channel.iterrows():
        values = (
            handle_dict(row.get('Channel_Names')),
            handle_dict(row.get('channel_Id')),
            handle_dict(row.get('Video_Id')),
            handle_dict(row.get('Title')),
            row.get('Tags'),  # Tags is probably a string or a list
            handle_dict(row.get('Thumbnail')),
            handle_dict(row.get('Description')),
            row.get('Published_Date'),
            row.get('Duration'),
            row.get('Views'),
            row.get('Comments'),
            row.get('Favorite_Count'),
            row.get('Definition'),
            row.get('Caption_Status')
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except mysql.connector.IntegrityError as ie:
            print(f"IntegrityError: Record with Video_Id {row['Video_Id']} already exists. Skipping.")
        except mysql.connector.DataError as de:
            print(f"DataError: {de}")
            print(f"Problematic data: {values}")
            mydb.rollback()
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")
            mydb.rollback()

    # Close the cursor and connection
    cursor.close()
    mydb.close()
    print("MySQL connection closed.")

# Call the function
videos_table("channel_name")  # Replace with an actual channel name if needed

#playlist table
import mysql.connector
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

def playlist_table(channel_name):

    # Function to convert datetime from ISO 8601 to MySQL format
    def convert_to_mysql_datetime(iso_datetime):
        try:
            return datetime.strptime(iso_datetime, "%Y-%m-%dT%H:%M:%SZ").strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            print(f"Error converting datetime: {e}")
            return None

    # Connect to MySQL
    print("Connecting to MySQL database...")
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        passwd='root',
        database='youtube',
        charset='utf8mb4'
    )
    cursor = mydb.cursor()
    print("Connected to MySQL database.")

    # Drop the table if it exists
    try:
        drop_query = '''DROP TABLE IF EXISTS playlists'''
        cursor.execute(drop_query)
        mydb.commit()
        print("Dropped existing playlists table (if it existed).")
    except Exception as e:
        print(f"Error dropping table: {e}")

    # Create the table
    try:
        create_query = '''CREATE TABLE IF NOT EXISTS playlists(
                            Playlist_Id VARCHAR(100) PRIMARY KEY,
                            Title VARCHAR(255) CHARACTER SET utf8mb4,
                            Channel_Id VARCHAR(100),
                            Channel_Name VARCHAR(100),
                            PublishedAt TIMESTAMP,
                            Video_Count INT
                        )'''
        cursor.execute(create_query)
        mydb.commit()
        print("Playlists table created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")

    # Connect to MongoDB
    print("Connecting to MongoDB...")
    
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    print("Connected to MongoDB.")

    # Fetch playlist information from MongoDB
    print("Fetching playlist information from MongoDB...")
    single_channel_details = []
    for ch_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        if "playlist_information" in ch_data:
            single_channel_details.extend(ch_data["playlist_information"])

    # Convert the fetched data into a DataFrame
    df_single_channel = pd.DataFrame(single_channel_details)

    # Convert the datetime strings to MySQL-compatible format
    if 'PublishedAt' in df_single_channel.columns:
        df_single_channel['PublishedAt'] = df_single_channel['PublishedAt'].apply(convert_to_mysql_datetime)

    # Insert DataFrame rows into the MySQL table
    insert_query = '''INSERT INTO playlists(
                        Playlist_Id,
                        Title,
                        Channel_Id,
                        Channel_Name,
                        PublishedAt,
                        Video_Count
                    ) VALUES (%s, %s, %s, %s, %s, %s)'''

    for index, row in df_single_channel.iterrows():
        values = (
            row['Playlist_Id'],
            row['Title'],
            row['Channel_Id'],
            row['Channel_Name'],
            row['PublishedAt'],
            row['Video_Count']
        )

        try:
            cursor.execute(insert_query, values)
            mydb.commit()
            print(f"Inserted Playlist_Id: {row['Playlist_Id']}")
        except mysql.connector.IntegrityError as ie:
            print(f"IntegrityError: Record with Playlist_Id {row['Playlist_Id']} already exists. Skipping.")
        except mysql.connector.DataError as de:
            print(f"DataError: {de}")
            print(f"Problematic data: {values}")
            mydb.rollback()
        except mysql.connector.Error as err:
            print(f"Error inserting data: {err}")
            mydb.rollback()

    # Close the cursor and connection
    cursor.close()
    mydb.close()
    print("MySQL connection closed.")

# Make sure to call the function
playlist_table("some_channel_name")  # Replace with an actual channel name if needed



import streamlit as st

def tables(channel_name):

    news= channels_table(channel_name)
    if news:
        st.write(news) 
    else:
        playlist_table(channel_name)
        videos_table(channel_name)
        comments_table(channel_name)

    return "Tables Created Successfully"
    





def show_channels_table():
    single_channel_details= []
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
            single_channel_details.append(ch_data["channel_information"])
    df= st.dataframe(single_channel_details)

    return df

def show_playlists_table():
    single_channel_details = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        single_channel_details.extend(ch_data["playlist_information"])

    # Convert the fetched data into a DataFrame
    df1 = st.dataframe(single_channel_details)
    return df1

def show_videos_table():
    single_channel_details = []  # List to hold all videos for all channels
    coll1 = db["channel_details"]

    for ch_data in coll1.find({}, {"_id": 0, "video_information": 1}):

        single_channel_details.extend(ch_data["video_information"])
    df2 = st.dataframe(single_channel_details)
    return df2


def show_comments_table():
    single_channel_details = []
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        if "comment_information" in ch_data:
            single_channel_details.extend(ch_data["comment_information"])


    df3 = st.dataframe(single_channel_details)
    
    return df3

#streamlit part
import warnings
warnings.filterwarnings("ignore", category=UserWarning, message="missing ScriptRunContext!")


with st.sidebar:
    st.title(":red[YOUTUBE DATA HAVERSTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("Data Collection")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["channel_Id"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)
#SQL Connection
mydb = mysql.connector.connect(
    host='localhost',
    user='root',
    passwd='root',
    database='youtube'
)
cursor = mydb.cursor()

question = st.selectbox("Select your question", (
    "1. All the videos and the channel name",
    "2. Channels with most number of videos",
    "3. Views of each channel",
    "4. Videos published in the year 2022",
    "5. Average duration of all videos in each channel",
    
))

if question == "1. All the videos and the channel name":
    query1 = '''SELECT title AS videos, channel_names AS channelname FROM videos'''
    cursor.execute(query1)
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1, columns=["Video Title", "Channel Names"])
    st.write(df)

elif question == "2. Channels with most number of videos":
    query2 = '''SELECT channel_names AS channelname, total_videos AS no_videos 
                FROM channels ORDER BY total_videos DESC'''
    cursor.execute(query2)
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["Channel Names", "No of Videos"])
    st.write(df2)





elif question == "3. Views of each channel":
    query7 = '''SELECT channel_names AS channelname, views AS totalviews FROM channels'''
    cursor.execute(query7)
    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["Channel Names", "Total Views"])
    st.write(df7)

elif question == "4. Videos published in the year 2022":
    query8 = '''SELECT title AS video_title, published_date AS videorelease, channel_names AS channelname 
                FROM videos WHERE EXTRACT(YEAR FROM published_date) = 2022'''
    cursor.execute(query8)
    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["Video Title", "Published Date", "Channel Names"])
    st.write(df8)

elif question == "5. Average duration of all videos in each channel":
    query9 = '''SELECT channel_names AS channelname, AVG(duration) AS averageduration 
                FROM videos GROUP BY channel_names'''
    cursor.execute(query9)
    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["Channel Names", "Average Duration"])

    # Formatting the average duration if necessary
    df9["Average Duration"] = df9["Average Duration"].apply(lambda x: str(x))
    st.write(df9)



# Closing the connection after the queries are done
cursor.close()
mydb.close()

import streamlit as st

from pymongo import MongoClient
import pymongo

client=pymongo.MongoClient("mongodb://subbu:vembu@ac-hxu0gpw-shard-00-00.udikzhc.mongodb.net:27017,ac-hxu0gpw-shard-00-01.udikzhc.mongodb.net:27017,ac-hxu0gpw-shard-00-02.udikzhc.mongodb.net:27017/?ssl=true&replicaSet=atlas-bvhqgr-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
db = client["Youtube_data"]

# Fetching all channel names from MongoDB
all_channels = []
coll1 = db["channel_details"]
for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
    try:
        all_channels.append(ch_data["channel_information"]["Channel_Names"])
    except KeyError:
        st.error("Error: 'Channel_Names' key not found in channel information")

# Streamlit selectbox to choose a channel
unique_channel = st.selectbox("Select the Channel", all_channels)

# Migrate to SQL if button is clicked
if st.button("Migrate to Sql"):
    try:
        Table = tables(unique_channel)  # Make sure `tables` is defined
        st.success(f"Migration successful for {unique_channel}")
    except Exception as e:
        st.error(f"Error during migration: {str(e)}")

# Show different tables based on user selection
show_table = st.radio("SELECT THE TABLE FOR VIEW", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    try:
        show_channels_table()  # Ensure this function is defined
    except Exception as e:
        st.error(f"Error displaying channels table: {str(e)}")

elif show_table == "PLAYLISTS":
    try:
        show_playlists_table()  # Ensure this function is defined
    except Exception as e:
        st.error(f"Error displaying playlists table: {str(e)}")

elif show_table == "VIDEOS":
    try:
        show_videos_table()  # Ensure this function is defined
    except Exception as e:
        st.error(f"Error displaying videos table: {str(e)}")

elif show_table == "COMMENTS":
    try:
        show_comments_table()  # Ensure this function is defined
    except Exception as e:
        st.error(f"Error displaying comments table: {str(e)}")
