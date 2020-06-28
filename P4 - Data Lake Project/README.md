# Data Warehouse for a music streaming App Sparkify

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the DataLake. . Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

#### Purpose of the database
In context of Sparkify, this Data Lake based ETL solution provides very elastic way of processing data. Spark uses in-memory processing so it creates schemas on read opposite to Redshift or Postgres schema of write. With increase in volume of data S3 provides an easy storage option as maintenance of the datastore is taken care by AWS unlike hadoop data store (HDFS) which requires manual configurations and upgrades.

#### - Task
Building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

#### - Datasets

Here there are two datasets that reside in S3. Following are the S3 links for each:

 * Song data: `s3://udacity-dend/song_data`
 * Log data: `s3://udacity-dend/log_data`

Log data json path: `s3://udacity-dend/log_json_path.json`

##### Song Dataset
Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.

```rust,ignore
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```

And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```json
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```


##### Log Dataset
The second dataset consists of log files in JSON forma. These are activity logs from the music streaming app.
The log files in the dataset are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```rust,ignore
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like

<img src="Images/log-data.png" width="700" height="200">


