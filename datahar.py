
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
api_key = "AIzaSyCNGeiXDtbGqn5Dn5I6H7m5exvzJpgDIcY"
#api_key = "AIzaSyAMW1l2tFN7r00v56cS0WLL6_uEHKCoPVs"
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
                      Tags=i.get('etag'),
                      Description=i['snippet'].get('description'),
                      Published_Date=i['snippet']['publishedAt'],
                      Duration=i['contentDetails']['duration'],
                      views=i['statistics'].get('viewCount'),
                      Comments=i['statistics'].get('commentCount'),
                      likes=i['statistics'].get('likeCount'),
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

db=client["Youtube_data"]
coll1= db["channel_details"]




def app():

    # st.title(":red[Get the Channel data]")
    # channel_id=st.text_input("Enter the channel ID")
    

    # if st.button("collect and store data"):
    #     ch_ids=[]
    #     db=client["Youtube_data"]
    #     coll1=db["channel_details"]
    #     for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    #         ch_ids.append(ch_data["channel_information"]["channel_Id"])

    #     if channel_id in ch_ids:
    #         st.success("Channel Details of the given channel id already exists")
    #         ch_ids=[]
    #         db=client["Youtube_data"]
    #         coll1=db["channel_details"]
    #         for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    #             ch_ids.append(ch_data["channel_information"])
    #             st.write(ch_data["channel_information"],channel_id)

    #     else:
    #         insert=channel_details(channel_id)
    #         st.success(insert)
    #         ch_ids=[]
    #         db=client["Youtube_data"]
    #         coll1=db["channel_details"]
    #         for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    #             ch_ids.append(ch_data["channel_information"])
    #             st.write(ch_ids)
            


# Assuming `client` and `channel_details` are properly initialized and imported

    st.title(":red[Get the Channel Data]")
    channel_id = st.text_input("Enter the channel ID")

    # Button to collect and store data
    if st.button("Collect and Store Data"):
        ch_ids = []
        
        # Connect to MongoDB and select the collection
        db = client["Youtube_data"]
        coll1 = db["channel_details"]
        
        # Get all the channel IDs from the database
        for ch_data in coll1.find({}, {"_id": 0, "channel_information.channel_Id": 1}):
            ch_ids.append(ch_data["channel_information"]["channel_Id"])

        # Check if the entered channel ID already exists in the database
        if channel_id in ch_ids:
            st.success("Channel details for the given channel ID already exist.")
            
            # Display the existing data for the given channel ID
            existing_data = coll1.find({"channel_information.channel_Id": channel_id}, {"_id": 0, "channel_information": 1})
            for data in existing_data:
                st.write(data["channel_information"])

        else:
            # If the channel doesn't exist, insert the new channel details
            insert = channel_details(channel_id)  # Assuming channel_details is a function you defined
            st.success(f"New channel details inserted: {insert}")
            
            # Optionally, display the newly inserted channel details
            new_data = coll1.find({"channel_information.channel_Id": channel_id}, {"_id": 0, "channel_information": 1})
            for data in new_data:
                st.write(data["channel_information"])
