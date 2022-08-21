import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id INT IDENTITY(0,1) PRIMARY KEY,
        artist_name VARCHAR,
        auth VARCHAR,
        user_first_name VARCHAR,
        user_gender  VARCHAR,
        item_in_session INT,
        user_last_name VARCHAR,
        song_length DOUBLE PRECISION,
        user_level VARCHAR,
        location VARCHAR,
        method VARCHAR,
        page VARCHAR,
        registration VARCHAR,
        session_id BIGINT,
        song_title VARCHAR,
        status INT,
        ts VARCHAR,
        user_agent TEXT,
        user_id VARCHAR)
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id VARCHAR PRIMARY KEY,
        num_songs INT,
        artist_id VARCHAR,
        artist_latitude DOUBLE PRECISION,
        artist_longitude DOUBLE PRECISION,
        artist_location VARCHAR,
        artist_name VARCHAR,
        title VARCHAR,
        duration DOUBLE PRECISION,
        year INT)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP,
        user_id VARCHAR,
        level VARCHAR,
        song_id VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        session_id BIGINT,
        location VARCHAR,
        user_agent TEXT)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY,
        title VARCHAR,
        artist_id VARCHAR NOT NULL,
        year INT,
        duration DOUBLE PRECISION)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        location VARCHAR,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events 
    from {}
    iam_role {}
    json {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs 
    from {} 
    iam_role {}
    json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT  
        TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
        e.user_id, 
        e.user_level, 
        s.song_id,
        s.artist_id, 
        e.session_id,
        e.location, 
        e.user_agent
    FROM staging_events e, staging_songs s
    WHERE e.page = 'NextSong' 
    AND e.song_title = s.title 
    AND e.artist_name = s.artist_name 
    AND e.song_length = s.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT  
        user_id, 
        user_first_name, 
        user_last_name, 
        user_gender, 
        user_level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekDay)
    SELECT start_time, 
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time), 
        EXTRACT(month from start_time),
        EXTRACT(year from start_time), 
        EXTRACT(dayofweek from start_time)
    FROM songplays
""")

# TEST QUERIES

test_query_1 = ("""
    SELECT artist_id, count(*) AS num_count
    FROM songs
    GROUP BY artist_id
    ORDER BY num_count DESC
    LIMIT 5
""")

test_query_2 = ("""
    SELECT level, count(*) AS num_count
    FROM users
    GROUP BY level
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]
test_queries = [test_query_1, test_query_2]
