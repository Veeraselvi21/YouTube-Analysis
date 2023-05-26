import streamlit as st
import googleapiclient.discovery
from googleapiclient.discovery import build
import googleapiclient.errors
import pymongo
from pymongo import MongoClient
import mysql.connector
import pandas as pd
import re

# Define your MongoDB connection details
mongo_uri = "mongodb://localhost:27017/"
mongo_db = "youtube"
mongo_collection = "videos"

# Create a MongoDB client
client = MongoClient(mongo_uri)
db = client[mongo_db]
collection = db[mongo_collection]

# Create a connection to the MySQL server
mysql_host = 'localhost'
mysql_user = 'root'
mysql_password = 'mysql@123'
mysql_database = 'youtube_data'
cnxn = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)

# Create a cursor
cursor = cnxn.cursor()

def get_channel_data(youtube, channel_id):
    request = youtube.channels().list(
        part='contentDetails,statistics,snippet',
        id=channel_id)
    response = request.execute()
    data = dict(
        ChannelName=response['items'][0]['snippet']['title'],
        ChannelId=response['items'][0]['id'],
        Subscribers=response['items'][0]['statistics']['subscriberCount'],
        TotalVideos=response['items'][0]['statistics']['videoCount'],
        ViewCount=response['items'][0]['statistics']['viewCount'],
        Description=response['items'][0]['snippet']['description'],
        Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    return data
    pass
def get_video_id(youtube, playlist_id):
    video_id = []
    response = youtube.playlistItems().list(
        part='snippet,contentDetails,id',
        playlistId=playlist_id,
        maxResults=50
    ).execute()

    while response:
        for item in response['items']:
            video_id.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        if next_page_token:
            response = youtube.playlistItems().list(
                part='snippet,contentDetails,id',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()
        else:
            break

    return video_id
    pass
def get_video_stats(youtube, video_ids):
    total_video_info = []

    for i in range(0, len(video_ids), 50):
        try:
            response = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=','.join(video_ids[i:i + 50])
            ).execute()
        except googleapiclient.errors.HttpError as error:
            pass
            results = None
        if response:
            for j in response['items']:
                video_stats = dict(videoId=j['id'],
                                   tag=j['etag'],
                                   Title=j['snippet']['title'],
                                   Description=j['snippet']['description'],
                                   Thumbnails=j['snippet']['thumbnails']['default']['url'],
                                   Published_date=j['snippet']['publishedAt'],
                                   Views=j['statistics']['viewCount'],
                                   Likes=j['statistics']['likeCount'],
                                   Comments=j['statistics']['commentCount'],
                                   )

                total_video_info.append(video_stats)
        return total_video_info
        pass

def get_dura_stats(youtube, video_ids):
        duration_info = {}

        for i in range(0, len(video_ids), 50):
            try:
                response = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=','.join(video_ids[i:i + 50])
                ).execute()
            except googleapiclient.errors.HttpError as error:
                pass
                results = None
            if response:
                for j in response['items']:
                    duration_string = j['contentDetails']['duration']
                    minutes = 0
                    seconds = 0

                    # Extract minutes and seconds using regular expressions
                    match = re.search(r'PT(\d+M)?(\d+S)?', duration_string)
                    if match:
                        minutes_str = match.group(1)
                        seconds_str = match.group(2)
                        if minutes_str:
                            minutes = int(minutes_str[:-1])
                        if seconds_str:
                            seconds = int(seconds_str[:-1])
                    duration_integer = minutes * 60 + seconds
                    duration_info[j['id']] = {'Duration': duration_integer}
        return duration_info
        pass

    # Function to get the comment details

def get_comments(youtube, video_ids):
        comments = []
        all_comment_stats = []

        for id in video_ids:
            try:
                results = youtube.commentThreads().list(
                    part="snippet,replies",
                    videoId=id
                ).execute()
            except googleapiclient.errors.HttpError as error:
                pass
                results = None

            if results:
                for item in results["items"]:
                    comment_stats = dict(comment=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                         CommentId=item['snippet']['topLevelComment']['id'],
                                         videoid=item['snippet']['videoId'],
                                         CommentPublished=item['snippet']['topLevelComment']['snippet']['publishedAt'],
                                         CommentText=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                         CommentAuthor=item['snippet']['topLevelComment']['snippet'][
                                             'authorDisplayName'])
                    comments.append(comment_stats)

        return comments
        pass


def store_youtube_data(channel_stats, video_ids, video_stats, comments, duration_list):
    data = {
        "Channel_Id": channel_stats["ChannelId"],
        "Channel_Name": channel_stats["ChannelName"],
        "Subscription_count": channel_stats["Subscribers"],
        "Channel_Views": channel_stats["ViewCount"],
        "Description": channel_stats["Description"],
        "PlaylistId": channel_stats["Playlist_id"],
        "Videos": []
    }

    for video_id in video_ids:
        video_data = {
            "VideoId": video_id,
            "VideoStats": {},
            "Comments": [],
            "Duration": 0  # Initialize duration to 0
        }

        for video_stat in video_stats:
            if video_stat["videoId"] == video_id:
                video_data["VideoStats"] = video_stat
                break

        for comment in comments:
            if comment["videoid"] == video_id:
                video_data["Comments"].append(comment)

        if video_id in duration_list:
            video_data["Duration"] = duration_list[video_id]["Duration"]

        data["Videos"].append(video_data)

    return data
    pass
def transfer_data_to_mysql(data):
    channel_stats = data
    channel_id = channel_stats["Channel_Id"]
    channel_name = channel_stats["Channel_Name"]
    subscription_count = channel_stats["Subscription_count"]
    channel_views = channel_stats["Channel_Views"]
    description = channel_stats["Description"]
    playlist_id = channel_stats["PlaylistId"]

    # Insert channel data into MySQL
    cursor.execute(
        "INSERT INTO channel (channel_Id, Channel_Name, Subscription_count, Channel_Views, Description) "
        "VALUES (%s, %s, %s, %s, %s)",
        (channel_id, channel_name, subscription_count, channel_views, description)
    )
    cnxn.commit()

    # Insert playlist data into MySQL
    cursor.execute(
        "INSERT INTO playlist (PlaylistId, Channel_Id) "
        "VALUES (%s, %s)",
        (playlist_id, channel_id)
    )
    cnxn.commit()

    # video details read from mongo
    query = {}
    projection = {
        '_id': 0,
        'PlaylistId': 1,
        'Videos': 1
    }
    documents = collection.find(query, projection)

    video_data = []

    # looping through each document
    for document in documents:
        playlist_id = document['PlaylistId']
        video_array = document['Videos']

        # Process each video in the array
        for video in video_array:
            video_id = video['VideoId']
            duration = video['Duration']
            video_stats = video['VideoStats']

            # Retrieve the nested fields
            title = video_stats.get('Title')
            description = video_stats.get('Description')
            published_date = video_stats.get('Published_date')
            views = video_stats.get('Views')
            likes = video_stats.get('Likes')
            comments = video_stats.get('Comments')
            thumbnails = video_stats.get('Thumbnails')

            # Getting the result in dictionary
            row = {
                'videoId': video_id,
                'PlaylistId': playlist_id,
                'Title': title,
                'Description': description,
                'Published_date': published_date,
                'Views': views,
                'Likes': likes,
                'Comments': comments,
                'Thumbnails': thumbnails,
                'Duration': duration
            }

            video_data.append(row)

    # Creating DataFrame
    df3 = pd.DataFrame(video_data)

    # Replace None values with 0
    df3.fillna(0, inplace=True)

    # Convert object type into preferred type
    df3['videoId'] = df3['videoId'].astype(str)
    df3['PlaylistId'] = df3['PlaylistId'].astype(str)
    df3['Title'] = df3['Title'].astype(str)
    df3['Description'] = df3['Description'].astype(str)
    df3['Published_date'] = pd.to_datetime(df3['Published_date'])
    df3['Views'] = df3['Views'].astype(int)
    df3['Likes'] = df3['Likes'].astype(int)
    df3['Comments'] = df3['Comments'].astype(int)
    df3['Thumbnails'] = df3['Thumbnails'].astype(str)

    # Insert into MySQL
    for _, row in df3.iterrows():
        insert_query = '''
            INSERT INTO video(videoId, PlaylistId, Title, Description, Published_date, Views, Likes, Comments, Thumbnails,Duration)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s)
        '''
        values = (
            row['videoId'],
            row['PlaylistId'],
            row['Title'],
            row['Description'],
            row['Published_date'].strftime('%Y-%m-%d %H:%M:%S'),
            row['Views'],
            row['Likes'],
            row['Comments'],
            row['Thumbnails'],
            row['Duration']
        )

        cursor.execute(insert_query, values)

    # Commit the changes
    cnxn.commit()

    query = {}
    projection = {
        '_id': 0,
        'Videos.Comments.CommentId': 1,
        'Videos.Comments.CommentPublished': 1,
        'Videos.Comments.videoid': 1,
        'Videos.Comments.CommentText': 1,
        'Videos.Comments.CommentAuthor': 1
    }
    documents = collection.find(query, projection)

    data = []
    for document in documents:
        for video in document['Videos']:
            for comment in video.get('Comments', []):
                row = {
                    'CommentId': comment.get('CommentId'),
                    'videoid': comment.get('videoid'),
                    'CommentText': comment.get('CommentText'),
                    'CommentAuthor': comment.get('CommentAuthor'),
                    'CommentPublished': comment.get('CommentPublished')
                }
                data.append(row)

    df4 = pd.DataFrame(data)
    df4['CommentId'] = df4['CommentId'].astype(str)
    df4['videoid'] = df4['videoid'].astype(str)
    df4['CommentText'] = df4['CommentText'].astype(str)
    df4['CommentAuthor'] = df4['CommentAuthor'].astype(str)
    df4['CommentPublished'] = pd.to_datetime(df4['CommentPublished'])
    for _, row in df4.iterrows():
        insert_query = '''
            INSERT INTO comment(CommentId, CommentPublished, videoid, CommentText, CommentAuthor)
            VALUES (%s, %s, %s, %s, %s)
        '''
        values = (
            row['CommentId'],
            row['CommentPublished'].strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(
                row['CommentPublished']) else '1970-01-01 00:00:00',
            row['videoid'],
            row['CommentText'],
            row['CommentAuthor']
        )

        cursor.execute(insert_query, values)

    # Commit the changes
    cnxn.commit()

    cursor.close()
    cnxn.close()


# Streamlit app layout
st.title("YouTube Data")
st.image("/Users/veera/Desktop/project_env/demo/logo.jpeg", use_column_width=True)

# Input fields for API key and channel ID
api_key = st.text_input("API Key")
channel_id = st.text_input("Channel ID")

# Submit button
if st.button("Submit"):
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        channel_stats = get_channel_data(youtube, channel_id)
        video_ids = get_video_id(youtube, channel_stats.get('Playlist_id'))
        video_stats = get_video_stats(youtube, video_ids)
        duration_list = get_dura_stats(youtube, video_ids)
        comments = get_comments(youtube, video_ids)
        data = store_youtube_data(channel_stats, video_ids, video_stats, comments, duration_list)
        collection.insert_one(data)
        transfer_data_to_mysql(data)  # Transfer data to MySQL
        st.success("Data inserted successfully!")
    except Exception as e:
        st.error(f"Error: {str(e)}")
