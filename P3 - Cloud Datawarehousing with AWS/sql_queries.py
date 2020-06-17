import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
(LOG_DATA, LOG_JSONPATH, SONG_DATA) = config["S3"].values()

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS ST_Events CASCADE;"
staging_songs_table_drop = "DROP TABLE IF EXISTS ST_Songs CASCADE;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

staging_events_table_create = """CREATE TABLE IF NOT EXISTS ST_Events (
                                artist VARCHAR,
                                auth VARCHAR,
                                firstName VARCHAR,
                                gender CHAR,
                                itemInSession INT,
                                lastName VARCHAR,
                                length REAL,
                                level VARCHAR,
                                location VARCHAR,
                                method CHAR(5),
                                page VARCHAR,
                                registration REAL,
                                sessionId INT,
                                song VARCHAR,
                                status INT,
                                ts BIGINT,
                                userAgent TEXT,
                                userId VARCHAR)
                                COMPOUND SORTKEY(userId,sessionId)
"""

staging_songs_table_create = """CREATE TABLE IF NOT EXISTS ST_Songs (
                artist_id VARCHAR NOT NULL SORTKEY,
                artist_latitude DECIMAL,
                artist_location VARCHAR,
                artist_longitude DECIMAL,
                artist_name VARCHAR,
                duration REAL,
                num_songs INT,
                song_id VARCHAR NOT NULL,
                title VARCHAR NOT NULL,
                year INT NOT NULL
)
"""

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays (
                songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
                start_time TIMESTAMP REFERENCES time(start_time),
                user_id VARCHAR NOT NULL REFERENCES users(user_id) DISTKEY,
                level VARCHAR NOT NULL,
                song_id VARCHAR REFERENCES songs(song_id),
                artist_id VARCHAR REFERENCES artists(artist_id),
                session_id INT NOT NULL,
                location TEXT,
                user_agent TEXT NOT NULL,
                UNIQUE(songplay_id,user_id,song_id,artist_id))
                    COMPOUND SORTKEY(user_id,session_id,level);
"""

user_table_create = """CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR PRIMARY KEY DISTKEY SORTKEY,
                first_name VARCHAR NOT NULL,
                last_name VARCHAR NOT NULL,
                gender CHAR(1) NOT NULL,
                level VARCHAR NOT NULL )
                ;
"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs (
                song_id VARCHAR PRIMARY KEY SORTKEY,
                song_title VARCHAR NOT NULL,
                artist_id VARCHAR NOT NULL,
                year int,
                song_duration DECIMAL,
                UNIQUE (song_id,artist_id))
                DISTSTYLE ALL;
"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists (
                artist_id VARCHAR PRIMARY KEY SORTKEY,
                artist_name VARCHAR NOT NULL,
                artist_location TEXT,
                artist_latitude NUMERIC,
                artist_longitude NUMERIC)
                DISTSTYLE ALL;
"""

time_table_create = """CREATE TABLE IF NOT EXISTS time (
                start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY SORTKEY DISTKEY,
                hour SMALLINT NOT NULL ,
                day SMALLINT NOT NULL ,
                week SMALLINT NOT NULL ,
                month SMALLINT NOT NULL,
                year INT NOT NULL,
                weekday SMALLINT NOT NULL);
"""

# STAGING TABLES

staging_events_copy = (
    """ COPY ST_Events from {}
                            credentials 'aws_iam_role={}'
                            format json {}
                            compupdate off
                            region 'us-west-2';"""
).format(LOG_DATA, IAM_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = (
    """ COPY ST_Songs from {}
                        credentials 'aws_iam_role={}'
                        json 'auto'
                        compupdate off region 'us-west-2';"""
).format(SONG_DATA, IAM_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = """ INSERT INTO songplays (start_time,
                                        user_id,
                                        level,
                                        song_id,
                                        artist_id,
                                        session_id,
                                        location,
                                        user_agent)
                        SELECT
						DISTINCT TIMESTAMP 'epoch' +  st_e.ts/1000 * interval '1 second' as start_time,
						st_e.userId,
						st_e.level,
						st_s.song_id,
						st_s.artist_id,
						st_e.sessionId,
						st_s.artist_location,
						st_e.userAgent
						FROM ST_songs st_s
						RIGHT JOIN ST_events st_e ON
						st_s.title = st_e.song AND st_s.duration=st_e.length AND st_s.artist_name = st_e.artist
						WHERE st_e.page = 'NextSong';
"""

user_table_insert = """
                        INSERT INTO users (
                        SELECT DISTINCT userId,
                        firstName,
                        lastName,
                        gender,
                        level
                        FROM ST_events
                        WHERE page = 'NextSong');
"""

song_table_insert = """ INSERT INTO songs (
                            SELECT DISTINCT song_id, title, artist_id, year, duration
                            FROM ST_songs
                        );
"""

artist_table_insert = """ INSERT INTO artists (
                            SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                            FROM ST_songs
                            );
"""

time_table_insert = """ INSERT INTO time (
                    select DISTINCT
                    TIMESTAMP 'epoch' +  ts/1000 * interval '1 second' as start_time,
                    extract (hour from start_time) as hour,
                    extract (day from start_time) as day,
                    extract (week from start_time) as week,
                    extract (month from start_time) as month,
                    extract (year from start_time) as year,
                    extract (weekday from start_time) as weekday
                    from st_events WHERE page = 'NextSong'
                    )
"""


# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
