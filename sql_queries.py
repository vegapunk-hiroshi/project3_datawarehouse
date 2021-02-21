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

# CREATE ANALYTICAL TABLES

songplay_table_create = ("""
CREATE TABLE songplay (
songplay_id      INT IDENTITY (1, 1) PRIMARY KEY, 
start_time       TIMESTAMP,
user_id          INT,
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
user_id          INT PRIMARY KEY,
firstName        VARCHAR, 
lastName         VARCHAR, 
gender           VARCHAR, 
level            VARCHAR
)
""")

song_table_create = ("""
CREATE TABLE song (
song_id          VARCHAR PRIMARY KEY,
title            VARCHAR, 
artist_id        VARCHAR, 
year             INT, 
duration         VARCHAR
)
""")

artist_table_create = ("""
CREATE TABLE artist (
artist_id        VARCHAR PRIMARY KEY,
name             VARCHAR,
location         VARCHAR, 
latitude         FLOAT, 
longitude        FLOAT
)
""")

time_table_create = ("""
CREATE TABLE time (
start_time       TIMESTAMP PRIMARY KEY,
hour             INT, 
day              INT, 
week             INT, 
month            INT, 
year             INT, 
weekday          INT
)
""")

## CREATE STAGING TABLE

staging_events_table_create = ("""
CREATE TABLE staging_events(
artist           VARCHAR,
auth             VARCHAR,  
firstName        VARCHAR,
gender           VARCHAR,
iteminSession    INT,
lastName         VARCHAR,
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
userId           INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
num_songs        INT,
artist_id        VARCHAR,
artist_latitude  FLOAT,
artist_longitude FLOAT,
artist_location  VARCHAR,
artist_name      VARCHAR,
song_id          VARCHAR ,
title            VARCHAR,
duration         VARCHAR,
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

# INSERT TABLES

songplay_table_insert = ("""
INSERT INTO songplay ( 
start_time, 
user_id, 
level, 
song_id, 
artist_id, 
sessionId, 
location, 
user_agent)
SELECT DISTINCT
(TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') AS start_time, 
        se.userId,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events se
        LEFT JOIN staging_songs ss
            ON se.song = ss.title
        WHERE se.page='NextSong' ;
""")

user_table_insert = ("""
INSERT INTO users (
user_id, 
firstName, 
lastName, 
gender, 
level
)
SELECT DISTINCT
se.userId,
se.firstName,
se.lastName,
se.gender,
se.level
FROM staging_events se 
WHERE page='NextSong' 
""")

song_table_insert = ("""
INSERT INTO song (
song_id,
title,
artist_id,
year,
duration
)
SELECT DISTINCT
ss.song_id,
ss.title,
ss.artist_id,
ss.year,
ss.duration
FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artist (
artist_id        ,
name             ,
location         , 
latitude        , 
longitude
)
SELECT DISTINCT 
ss.artist_id,
ss.artist_name,
se.location,
ss.artist_latitude,
ss.artist_longitude
FROM staging_events se JOIN staging_songs ss
ON (se.artist=ss.artist_name AND
se.song=ss.title AND
se.length=ss.duration)""")

time_table_insert = ("""
INSERT INTO time (
start_time       ,
hour             , 
day              , 
week             , 
month            , 
year             , 
weekday          
)
SELECT DISTINCT 
start_time,
EXTRACT(HOUR FROM start_time) AS hour,
EXTRACT(DAY FROM start_time) AS day,
EXTRACT(WEEK FROM start_time) AS week,
EXTRACT(MONTH FROM start_time) AS month,
EXTRACT(YEAR FROM start_time) AS year,
EXTRACT(DOW FROM start_time) AS weekday
FROM (SELECT DISTINCT '1970-01-01'::date + ts/1000 * interval '1 second' as start_time
FROM staging_events) """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
