import mysql.connector
import pandas as pd
import pymongo
from pymongo import MongoClient
# create connection to the MySQL server
cnxn = mysql.connector.connect(
    host='localhost',
    user='User_name',
    password='Your_password',
    database = 'Your_Databse'
)
# Creating a cursor
cursor = cnxn.cursor()
# Connect to MongoDB server
client = MongoClient('mongodb://localhost:27017/')

# Select the database and collection
db = client['database_name']
collection = db['collection_name']

# CREATING A TABLE channel
create_table_query = '''
     CREATE TABLE IF NOT EXISTS channel (
        Channel_Id VARCHAR(255) PRIMARY KEY,
        Channel_Name VARCHAR(255),
        Subscription_count INT,
        Channel_Views BIGINT,
        Description TEXT
    );
'''
cursor.execute(create_table_query, multi = False)
cnxn.commit()
cnxn.close()

# CREATING A TABLE playlist
create_table_query = '''
     CREATE TABLE IF NOT EXISTS playlist(
        PlaylistId VARCHAR(255) PRIMARY KEY,
        Channel_Id VARCHAR(255),
        FOREIGN KEY(Channel_Id) REFERENCES channel(Channel_Id)
    );
'''
cursor.execute(create_table_query, multi = False)
cnxn.commit()
cnxn.close()

# CREATING A TABLE video
create_table_query = '''
     CREATE TABLE IF NOT EXISTS video(
        VideoId VARCHAR(255) PRIMARY KEY,
        PlaylistId VARCHAR(255),
        Title VARCHAR(255),
        Description TEXT,
        Published_date DATETIME,
        Views BIGINT,
        Likes BIGINT,
        Comments BIGINT,
        Thumbnails VARCHAR(255),
        Duration INT,
        FOREIGN KEY(PlaylistId) REFERENCES playlist(PlaylistId)
    );
'''
cursor.execute(create_table_query, multi = False)
cnxn.commit()
cnxn.close()

# CREATING A TABLE comment
create_table_query = '''
     CREATE TABLE IF NOT EXISTS comment(
    CommentId VARCHAR(255) PRIMARY KEY,
    videoid VARCHAR(255),
    CommentText TEXT,
    CommentAuthor VARCHAR(255),
    CommentPublished DATETIME,
    FOREIGN KEY(videoid) REFERENCES video(Id)
    );
'''
cursor.execute(create_table_query, multi = False)
cnxn.commit()
cnxn.close()

# Channel details read from mongo and insert into Mysql

query = {}
projection = {
    '_id': 0,
    'Channel_Id': 1,
    'Channel_Name': 1,
    'Subscription_count': 1,
    'Channel_Views': 1,
    'Description': 1
}
channel_documents = collection.find(query, projection)

#making it as a pandas DataFrame
df = pd.DataFrame(list(channel_documents))

# Get the values
values = df[['Channel_Id', 'Channel_Name', 'Subscription_count', 'Channel_Views', 'Description']].values.tolist()

# INSERT INTO statement
insert_query = '''
    INSERT INTO channel (Channel_Id, Channel_Name, Subscription_count, Channel_Views, Description)
    VALUES (%s, %s, %s, %s, %s)
'''

# Execute and commit
cursor.executemany(insert_query, values)
cnxn.commit()

# Close the cursor and connection
cursor.close()
cnxn.close()

#Playlist details read from mongo and insert into Mysql

import pandas as pd
from pymongo import MongoClient

# Connect to MongoDB server
client = MongoClient('mongodb://localhost:27017/')

# Select the database and collection
db = client['youtube']
collection = db['videos']

query = {}
projection = {
    '_id': 0,
    'PlaylistId': 1,
    'Channel_Id': 1,
}

video_documents = collection.find(query, projection)

df1 = pd.DataFrame(list(video_documents))


# Get the values from the DataFrame as a list of tuples
values = df1[['PlaylistId','Channel_Id']].values.tolist()

# INSERT INTO statement
insert_query = '''
    INSERT INTO playlist(PlaylistId, Channel_Id)
    VALUES (%s, %s)
'''


cursor.executemany(insert_query, values)
cnxn.commit()
cursor.close()
cnxn.close()

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
# Convert object type into preferred type

df3['videoId'] = df3['videoId'].astype(str)
df3['PlaylistId'] = df3['PlaylistId'].astype(str)
df3['Title'] = df3['Title'].astype(str)
df3['Description'] = df3['Description'].astype(str)
df3['Published_date'] = pd.to_datetime(df3['Published_date'])
df3['Views'] = df3['Views'].fillna(0).astype('int64')
df3['Likes'] = df3['Likes'].fillna(0).astype('int64')
df3['Comments'] = df3['Comments'].fillna(0).astype('int64')
df3['Thumbnails'] = df3['Thumbnails'].astype(str)
# Create connection to the MySQL server
cnxn = mysql.connector.connect(
    host='localhost',
    user='username',
    password='password',
    database='youtube_data'
)

cursor = cnxn.cursor()

# insert into Mysql

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
        row['Published_date'].strftime('%Y-%m-%d %H:%M:%S') if not pd.isnull(
            row['Published_date']) else '1970-01-01 00:00:00',
        row['Views'],
        row['Likes'],
        row['Comments'],
        row['Thumbnails'],
        row['Duration']
    )

    cursor.execute(insert_query, values)
cnxn.commit()
cursor.close()
cnxn.close()
# reading from mongo db
import pandas as pd
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client['youtube']
collection = db['videos']

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
# write /insert into my sql
cnxn = mysql.connector.connect(
    host='localhost',
    user='user',
    password='password',
    database='youtube_data'
)

# Creating a cursor
cursor = cnxn.cursor()

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

cnxn.commit()

cursor.close()
cnxn.close()


