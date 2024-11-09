import streamlit as st
import mysql.connector
import pandas as pd

def app():
    
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
        "6. 10 most viewed videos",
        "7. comments in each videos",
        "8. Videos with higest likes",
        "9. likes of all videos", 
        "10. videos with highest number of comments" 
        
        
        
        
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
        query3 = '''SELECT channel_names AS channelname, views AS totalviews FROM channels'''
        cursor.execute(query3)
        t7 = cursor.fetchall()
        df3 = pd.DataFrame(t7, columns=["Channel Names", "Total Views"])
        st.write(df3)

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
        
    
    elif question=="6. 10 most viewed videos":
        query10='''select views as Views,Channel_Names ,Title as Video_Title from videos 
                    where views is not null order by views desc limit 10'''
        cursor.execute(query10)
        t10=cursor.fetchall()
        df10=pd.DataFrame(t10,columns=["Views","Channel Names","Video_Title"])
        st.write(df10)
    
    elif question=="7. comments in each videos":
        query11='''select comments as No_comments,title as Video_Title from videos where comments is not null'''
        cursor.execute(query11)
        t4=cursor.fetchall()
        df11=pd.DataFrame(t4,columns=["no of comments","videotitle"])
        st.write(df11)

    elif question=="8. Videos with higest likes":
        query12='''select Title as Video_Title, Channel_Names as Channel_Names  ,likes as likecount
                    from videos where likes is not null order by likes desc'''
        cursor.execute(query12)
        t5=cursor.fetchall()
        df12=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
        st.write(df12)

    elif question=="9. likes of all videos":
        query13='''select likes as like_count,Title as Video_Title from videos'''
        cursor.execute(query13)
        t13=cursor.fetchall()
        df13=pd.DataFrame(t13,columns=["likecount","videotitle"])
        st.write(df13)
    
    elif question=="10. videos with highest number of comments":
        query14='''select Title as video_Title, Channel_Names,Comments from videos where Comments is
                    not null order by Comments desc'''
        cursor.execute(query14)
        t10=cursor.fetchall()
        df14=pd.DataFrame(t10,columns=["video title","channel name","comments"])
        st.write(df14)



    cursor.close()
    mydb.close()