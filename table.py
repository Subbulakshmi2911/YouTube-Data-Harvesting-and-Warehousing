import streamlit as st
import pandas as pd
from pymongo import MongoClient

client=MongoClient("mongodb://subbu:vembu@ac-hxu0gpw-shard-00-00.udikzhc.mongodb.net:27017,ac-hxu0gpw-shard-00-01.udikzhc.mongodb.net:27017,ac-hxu0gpw-shard-00-02.udikzhc.mongodb.net:27017/?ssl=true&replicaSet=atlas-bvhqgr-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
 
db=client["Youtube_data"]
coll1= db["channel_details"]   

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


# Show different tables based on user selection


def app():
    st.title(":red[SELECT THE TABLE FOR VIEW]")
    show_table = st.radio( "Select the table",("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

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
        