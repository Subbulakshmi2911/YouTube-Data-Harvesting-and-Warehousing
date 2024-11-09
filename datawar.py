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


import pymongo

client=pymongo.MongoClient("mongodb://subbu:vembu@ac-hxu0gpw-shard-00-00.udikzhc.mongodb.net:27017,ac-hxu0gpw-shard-00-01.udikzhc.mongodb.net:27017,ac-hxu0gpw-shard-00-02.udikzhc.mongodb.net:27017/?ssl=true&replicaSet=atlas-bvhqgr-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")

import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

# Connect to the MongoDB database
db = client["Youtube_data"]


db=client["Youtube_data"]
coll1= db["channel_details"]



def channels_table(channel_name):
    # Connect to MySQL
    mydb = mysql.connector.connect(
        host='localhost',
        user='root',
        password='root',
        database='youtube',
        charset='utf8mb4' 
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
                            Chennal_Description	 TEXT CHARACTER SET utf8mb4,
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
                        Chennal_Description	,
                        Playlist_Id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''

    for index, row in df_single_channel.iterrows():
        values = (
            row.get('Channel_Names'),
            row.get('channel_Id'),
            row.get('subscribers'),
            row.get('Views'),
            row.get('Total_Videos'),
            row.get('Chennal_Description', ''),
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
 # Replace with an actual channel name if needed



# Comment table
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
                            Description TEXT CHARACTER SET utf8mb4,
                            Published_Date DATETIME NULL,
                            Duration TIME NULL,
                            views INT,
                            Comments INT,
                            likes INT,
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
                        Description,
                        Published_Date,
                        Duration,
                        views,
                        Comments,
                        likes,
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
            handle_dict(row.get('Description')),
            row.get('Published_Date'),
            row.get('Duration'),
            row.get('views'),
            row.get('Comments'),
            row.get('likes'),
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



def insertdata():
        channels_table("channel_name")
        st.markdown(f'<p style="color: gray;">Channel table inserted</p>', unsafe_allow_html=True)
        comments_table("channel_name")
        st.markdown(f'<p style="color: gray;">Comments table inserted</p>', unsafe_allow_html=True)
        videos_table("channel_name")
        st.markdown(f'<p style="color: gray;">Videos table inserted</p>', unsafe_allow_html=True)
        playlist_table("channel_name")
        st.markdown(f'<p style="color: gray;">Playlist table inserted</p>', unsafe_allow_html=True)

def selectchannel():
    all_channels = []
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        try:
            # Ensure 'Channel_Names' exists in channel_information
            channel_name = ch_data["channel_information"].get("Channel_Names")
            if channel_name:
                all_channels.append(channel_name)
                
                
            else:
                print(f"Missing 'Channel_Names' in a channel document: {ch_data}")
        except KeyError:
            print("Error: 'channel_information' or 'Channel_Names' key not found in channel document")
    return all_channels

   

    # Define the migration function (you should implement this)
def migrate_to_sql(channel_name):
    try:
        st.write(f"Data for {channel_name} migrated to SQL successfully.")
    except Exception as e:
        st.error(f"Error during migration: {str(e)}")

def app():
    st.title(":red[Data Migration from MongoDB to MySQL]")
   
    
    # Allow the user to select a channel
    selected_channel = st.selectbox("Select the Channel", selectchannel())

    if st.button('Migrate Data'):
        insertdata()  # Call the data insertion function
        st.markdown(f'<p style="color: green;">Data for {selected_channel} migrated to SQL successfully.</p>', unsafe_allow_html=True)
   


# if __name__ == '__main__':
#     app()




