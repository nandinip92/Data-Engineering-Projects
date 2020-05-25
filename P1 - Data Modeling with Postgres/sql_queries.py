# DROP TABLES

songplay_table_drop = "DROP TABLE songplays CASCADE"
user_table_drop = "DROP TABLE users CASCADE"
song_table_drop = "DROP TABLE songs CASCADE"
artist_table_drop = "DROP TABLE artists CASCADE"
time_table_drop = "DROP TABLE time CASCADE"

# CREATE TABLES

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays (
                songplay_id BIGSERIAL PRIMARY KEY, 
                start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL REFERENCES time(start_time), 
                user_id VARCHAR NOT NULL REFERENCES users(user_id), 
                level VARCHAR NOT NULL, 
                song_id VARCHAR REFERENCES songs(song_id), 
                artist_id VARCHAR REFERENCES artists(artist_id), 
                session_id INT NOT NULL, 
                location TEXT, 
                user_agent TEXT NOT NULL,
                UNIQUE(songplay_id,user_id,song_id,artist_id))
"""
#
# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

user_table_create = """CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR, 
                first_name VARCHAR NOT NULL, 
                last_name VARCHAR NOT NULL, 
                gender CHAR(1) NOT NULL CHECK(gender = 'F' OR gender = 'M'), 
                level VARCHAR NOT NULL CHECK(level = 'free' OR level = 'paid'),
                PRIMARY KEY (user_id));
"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs (
                song_id VARCHAR PRIMARY KEY, 
                song_title VARCHAR NOT NULL, 
                artist_id VARCHAR NOT NULL, 
                year int, 
                song_duration DECIMAL,
                UNIQUE (song_id,artist_id));
"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists (
                artist_id VARCHAR PRIMARY KEY, 
                artist_name VARCHAR NOT NULL, 
                location TEXT, 
                latitude NUMERIC, 
                longitude NUMERIC);

"""

time_table_create = """CREATE TABLE IF NOT EXISTS time (
                start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY, 
                hour SMALLINT NOT NULL CHECK (hour <=24), 
                day SMALLINT NOT NULL CHECK (day <=31), 
                week SMALLINT NOT NULL , 
                month SMALLINT NOT NULL CHECK(month <= 12), 
                year INT NOT NULL, 
                weekday SMALLINT NOT NULL);
"""

# INSERT RECORDS

songplay_table_insert = """
INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) \
VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
"""

user_table_insert = """INSERT INTO users(
user_id, first_name, last_name, gender, level) VALUES (%s,%s,%s,%s,%s) ON CONFLICT (user_id)
DO UPDATE
    SET level = EXCLUDED.level
"""

song_table_insert = """INSERT INTO songs(
song_id, song_title, artist_id, year, song_duration) VALUES (%s,%s,%s,%s,%s)  ON CONFLICT DO NOTHING
"""

artist_table_insert = """INSERT INTO artists(
artist_id, artist_name, location, latitude, longitude) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING
"""


time_table_insert = """INSERT INTO  time(
start_time, hour, day, week, month, year, weekday) VALUES (%s,%s,%s,%s,%s,%s,%s)  ON CONFLICT DO NOTHING
"""

# FIND SONGS

song_select = """
        SELECT  song_id,artist_id FROM songs NATURAL JOIN artists \
        WHERE song_title = %s AND artist_name = %s AND song_duration = %s;
"""

# QUERY LISTS

create_table_queries = [
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
    songplay_table_create,
]
drop_table_queries = [
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]
