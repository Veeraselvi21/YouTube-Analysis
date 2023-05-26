# importing all the libraries

import googleapiclient.discovery
from googleapiclient.discovery import build
import googleapiclient.errors
import pandas as pd
import re

# setting the API connection

api_key = 'API-KEY'  # API key from developer console
channel_id = 'Channel_ID'

youtube = build('youtube', 'v3', developerKey=api_key)  # Youtube


# Getting channel details from youtube api

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


channel_stats = get_channel_data(youtube, channel_id)

# function to get video details from playlist id

playlist_id = channel_stats.get('Playlist_id')  # getting playlist_id from the dictionary


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


video_ids = get_video_id(youtube, playlist_id)

def get_video_stats(youtube, video_ids):
    total_video_info = []

    for i in range(0, len(video_ids), 50):
        try:
            response = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=','.join(video_ids[i:i+50])
            ).execute()
        except googleapiclient.errors.HttpError as error:
            pass
            results = None
        if response:
            for j in response['items']:
                video_stats = dict(Id=j['id'],
                                   tag=j['etag'],
                                   Title=j['snippet']['title'],
                                   Description=j['snippet']['description'],
                                   Thumbnails=j['snippet']['thumbnails']['default']['url'],
                                   Published_date=j['snippet']['publishedAt'],
                                   Views=j['statistics']['viewCount'],
                                   Likes=j['statistics'].get('likeCount', 0),
                                   Comments=j['statistics'].get('commentCount',0)
                                   )

                total_video_info.append(video_stats)

    return total_video_info
video_stats = get_video_stats(youtube, video_ids)

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
duration_list= get_dura_stats(youtube, video_ids)


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
                                     CommentAuthor=item['snippet']['topLevelComment']['snippet']['authorDisplayName'])
                comments.append(comment_stats)

    return comments


comments = get_comments(youtube, video_ids)

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
            "Duration": 0
        }

        for video_stat in video_stats:
            if video_stat["Id"] == video_id:
                video_data["VideoStats"] = video_stat
                break

        for comment in comments:
            if comment["videoid"] == video_id:
                video_data["Comments"].append(comment)

        if video_id in duration_list:
            video_data["Duration"] = duration_list[video_id]["Duration"]


        data["Videos"].append(video_data)

    return data
data = store_youtube_data(channel_stats, video_ids, video_stats, comments, duration_list)
