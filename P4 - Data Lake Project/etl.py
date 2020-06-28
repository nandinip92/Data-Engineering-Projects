import configparser
from typing import Optional
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, max, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType, StringType


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    """
    Funtion to create a spark session to process data
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    return spark


def printStatement(table_name: str, input_data: Optional[str] = ""):
    """
    Function to just print a loading statement
    """
    print("Loading {} data in {}{}".format(table_name, input_data, table_name))


def process_song_data(spark, input_data, output_data):
    """
    Funtion to  1. read the songs_data (JSON files) from the S3 bucket path.
                2. extracting the respective columns data for `songs` and `artists` tables.
                3. writing the table into parquet files and loading into a S3 bucket.
    """
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*/*.json"

    # reading song data file
    df = spark.read.json(song_data)

    # extracting columns to create songs table
    songs_table = df.selectExpr(
        ["song_id", "title", "artist_id", "cast(year as int) year", "duration"]
    ).orderBy("song_id")

    printStatement("songs", output_data)
    # writing songs table to parquet files partitioned by year and artist
    songs_table.printSchema()
    songs_table.write.mode("append").partitionBy("year", "artist_id").parquet(
        output_data + "songs/"
    )

    printStatement("artists", output_data)
    # extracting columns to create artists table
    artists_table = df.select(
        [
            "artist_id",
            "artist_name",
            "artist_location",
            "artist_latitude",
            "artist_longitude",
        ]
    ).orderBy("artist_id")

    # writing artists table to parquet files
    artists_table.printSchema()
    artists_table.write.mode("append").parquet(output_data + "artists/")



def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filteing by actions for song plays i.e.,records with page `NextSong`
    df = df.filter(col("page") == "NextSong")

    printStatement("users", output_data)
    # extracting columns for users table
    users_table = df.selectExpr(
        "userId as user_id",
        "firstName as first_name",
        "lastName as last_name",
        "gender",
        "level",
    ).dropDuplicates()

    # writing users table (without duplicates) to parquet files
    users_table.printSchema()
    users_table.write.mode("append").parquet(output_data + "users/")

    printStatement("time", output_data)
    # creating timestamp column from original timestamp column
    get_timestamp = udf(
        lambda epoch: datetime.fromtimestamp(epoch / 1000), TimestampType()
    )
    df = df.withColumn("timestamp", get_timestamp("ts"))

    # create datetime column from original timestamp column
    # get_datetime = udf(lambda epoch: datetime.fromtimestamp(epoch/1000),DateType())
    get_datetime = udf(
        lambda epoch: datetime.fromtimestamp(epoch / 1000).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
        StringType(),
    )
    df = df.withColumn("date_time", get_datetime("ts"))

    # extracting columns to create time table
    time_table = (
        df.selectExpr(
            "timestamp as start_time",
            "hour(timestamp) as hour",
            "dayofmonth(timestamp) as day",
            "weekofyear(timestamp) as week",
            "month(timestamp) as month",
            "year(timestamp) as year",
            "dayofweek(timestamp) as weekday",
        )
        .distinct()
        .orderBy("timestamp")
    )

    # writing time table to parquet files partitioned by year and month
    time_table.printSchema()
    time_table.write.mode("append").partitionBy("year", "month").parquet(
        output_data + "time/"
    )

    printStatement("songplays", output_data)
    # reading in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json").select(
        ["artist_id", "song_id", "artist_name", "title", "duration"]
    )

    # Selecting required columns from log data
    log_df = df.selectExpr(
        "artist",
        "song",
        "length",
        "timestamp as start_time",
        "level",
        "location",
        "cast(sessionId as int) as session_id",
        "userId as user_id",
        "userAgent as user_agent",
    )

    # Joining songs data and log data
    joined_df = log_df.join(
        song_df,
        (log_df.artist == song_df.artist_name)
        & (log_df.song == song_df.title)
        & (log_df.length == song_df.duration),
        how="left",
    )

    # Adding id column using monotonically_increasing_id
    joined_df = joined_df.withColumn("songplay_id", monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = joined_df.selectExpr(
        [
            "cast(songplay_id as bigint) as songplay_id",
            "start_time",
            "user_id",
            "level",
            "song_id",
            "artist_id",
            "session_id",
            "location",
            "user_agent",
            "year(start_time) as year",
            "month(start_time) as month",
        ]
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.printSchema()
    songplays_table.write.mode("append").partitionBy("year", "month").parquet(
        output_data + "songplays/"
    )


def main():
    """
        1.Creates a Spark session
        2. Processes song_data
        3. Processes log_data
    """
    spark = create_spark_session()
    #input_data = config["S3_BUCKET"]["INPUT_PATH"]
    #output_data = config["S3_BUCKET"]["OUTPUT_PATH"]

    input_data = config["LOCAL"]["INPUT_PATH"]
    output_data = config["LOCAL"]["OUTPUT_PATH"]

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    print("Completed ETL Process....")


if __name__ == "__main__":
    main()
