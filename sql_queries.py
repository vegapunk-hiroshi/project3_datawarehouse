import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE songplay (
songplay_id      VARCHAR, 
start_time       TIMESTAMP,
user_id          INT        NOT NULL,
level            VARCHAR,
song_id          VARCHAR,
artist_id        VARCHAR,
sessionId        INT,
location         VARCHAR, 
user_agent       VARCHAR
)
""")

user_table_create = ("""
CREATE TABLE users (
user_id          INT,
firstName        VARCHAR, 
lastName         VARCHAR, 
gender           VARCHAR, 
level            VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE song (
song_id          VARCHAR, 
title            VARCHAR, 
artist_id        VARCHAR, 
year             INT, 
duration         VARCHAR
)
""")

artist_table_create = ("""
CREATE TABLE artist (
artist_id        VARCHAR,
name             VARCHAR,
location         VARCHAR, 
lattitude        FLOAT, 
longitude        FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time (
start_time       TIMESTAMP,
hour             INT, 
day              INT, 
week             INT, 
month            INT, 
year             INT, 
weekday          INT
)
""")


staging_events_table_create = ("""
CREATE TABLE staging_events(
artist           VARCHAR,
auth             VARCHAR  NOT NULL,  
firstName        VARCHAR  NOT NULL,
gender           VARCHAR,
iteminSession    INT      NOT NULL,
lastName         VARCHAR  NOT NULL,
length           FLOAT,
level            VARCHAR,
location         VARCHAR,
method           VARCHAR,
page             VARCHAR,
registration     VARCHAR,
sessionId        INT,
song             VARCHAR,
status           INT,
ts               BIGINT,
userAgent        VARCHAR,
userId           INT      NOT NULL
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
num_songs        INT      NOT NULL,
artist_id        VARCHAR  NOT NULL,
artist_latitude  FLOAT,
artist_longitude FLOAT,
artist_location  VARCHAR,
artist_name      VARCHAR  NOT NULL,
song_id          VARCHAR  NOT NULL,
title            VARCHAR  NOT NULL,
duration         VARCHAR  NOT NULL,
year             INT
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2' 
    timeformat as 'epochmillisecs' format as json {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE','ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    COMPUPDATE OFF region 'us-west-2' 
    json 'auto'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (
songplay_id, start_time, userId, level, song_id, artist_id, sessionId, location, user_agent)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
""")


user_table_insert = ("""

""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
