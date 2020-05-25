import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Function to process Song Dataset.
        This take the cursor object and each JSON filepath as input arguments.
        Both songs and artists tables are being populated.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[
        ["song_id", "title", "artist_id", "year", "duration"]
    ].values.tolist()[0]
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ].values.tolist()[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Funtion to process Log Dataset. 
        This take the cursor object and each JSON filepath as input arguments.
        Tables - time,users and songplay are being populated.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df.query('page == "NextSong"')

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = (
        "timestamp",
        "hour",
        "day",
        "week_of_year",
        "month",
        "year",
        "weekday",
    )
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    songplay_result = []
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
        except psycopg2.Error as e:
            print("------>", e)
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit="ms"),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )

        songplay_result.append(songplay_data)

    # creating a data frame for the songplay dataset
    sp_column_labels = [
        "start_time",
        "user_id",
        "level",
        "song_id",
        "artist_id",
        "session_id",
        "location",
        "user_agent",
    ]
    songplay_df = pd.DataFrame(songplay_result, columns=sp_column_labels)

    # Using the COPY for Bulk loading
    copy_query = """COPY songplays (start_time, user_id, \
                    level, song_id, artist_id, session_id, location, \
                    user_agent) FROM STDIN WITH CSV HEADER DELIMITER AS ',' 
                """
    songplay_df.to_csv("songplay_df.csv", sep=",", index=False, encoding="utf-8")
    songplay_file = open("songplay_df.csv", "r")

    try:
        cur.copy_expert(sql=copy_query, file=songplay_file)
    except psycopg2.Error as e:
        print("Error in Inserting SONG_PLAY for file :{}".format(filepath))
        print(e)


def process_data(cur, conn, filepath, func):
    """
    Funtion to get all JSON filepaths from the given song_data and log_data paths.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, "*.json"))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print("{} files found in {}".format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print("{}/{} files processed.".format(i, num_files))


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=student password=student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
