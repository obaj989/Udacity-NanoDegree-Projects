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
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist varchar,
        auth varchar not null,
        firstName varchar,
        gender varchar,
        itemInSession int not null,
        lastName varchar,
        length float,
        level varchar not null,
        location varchar,
        method varchar not null,
        page varchar not null,
        registration varchar,
        sessionId int not null,
        song varchar,
        status int not null,
        ts text not null,
        userAgent varchar,
        userId int
);
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs int not null,
        artist_id varchar not null,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar not null,
        song_id varchar not null,
        title varchar not null,
        duration float not null,
        year int not null
);
""")

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id int identity(0, 1) PRIMARY KEY distkey,
        start_time timestamp not null,
        user_id int not null,
        level varchar not null,
        song_id varchar,
        artist_id varchar,
        session_id int not null,
        location varchar,
        user_agent varchar not null
    )sortkey auto;
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id int primary key,
        first_name varchar not null,
        last_name varchar not null,
        gender varchar not null,
        level varchar not null
    )diststyle all sortkey auto;
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id varchar primary key distkey,
        title varchar not null,
        artist_id varchar not null,
        year int not null,
        duration numeric not null
    )sortkey auto;
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id varchar primary key distkey,
        name varchar not null,
        location varchar,
        latitude numeric,
        longitude numeric
    )sortkey auto;
""")

time_table_create = ("""
    CREATE TABLE times (
        start_time timestamp primary key,
        hour int not null,
        day int not null,
        week int not null,
        month int not null,
        year int not null,
        weekday int not null
    )sortkey auto;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    iam_role '{}'
    REGION 'us-west-2' 
    FORMAT AS JSON {}
""").format(config.get("S3", "LOG_DATA"),
            config.get("IAM_ROLE", "ARN"),
            config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    iam_role '{}'
    REGION 'us-west-2'
    JSON 'auto'
""").format(config.get("S3", "SONG_DATA"),
            config.get("IAM_ROLE", "ARN"))
# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, user_id, level, song_id, artist_id,
        session_id, location, user_agent
    )
    SELECT
        TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events e
    LEFT JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name
    WHERE e.page = 'NextSong'
""")
user_table_insert = ("""
    INSERT INTO users (user_id,first_name, last_name, gender, level)
    SELECT  userID, firstName, lastName, gender, level FROM (
        SELECT MAX(ts, userID, firstName, lastName, gender, level 
        FROM staging_events WHERE page = 'NextSong' AND userID is not null)
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    WITH uniq_staging_events AS (
        SELECT userID, firstName, lastName, gender, level,
        ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
        FROM staging_events WHERE userID IS NOT NULL)
    SELECT userID, firstName, lastName, gender, level 
    FROM uniq_staging_events WHERE rank = 1;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location,
                    artist_latitude, artist_longitude
    FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO times (start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        start_time,
        EXTRACT(HOUR FROM start_time),
        EXTRACT(DAY FROM start_time),
        EXTRACT(WEEK FROM start_time),
        EXTRACT(MONTH FROM start_time),
        EXTRACT(YEAR FROM start_time),
        EXTRACT(WEEKDAY FROM start_time) 
    FROM songplays
""")
# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
