## Data Modeling with Cassandra and Building ETL pipeline for anaylising data for a Music Streaming App 

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, the data reside in a directory of CSV files on user activity on the app.

#### Datasets

working with one dataset: `event_data`. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
<!-- ignore: syntax fragment -->
```rust,ignore
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```
#### ETL Steps
The ETL pipeline and data modeling are written in a single jupyter notebook, `Project_1B_Project_Template.ipynb`.
Following are the steps for processing `CSV` files - `events_data` and loading into ***Cassandra***.
   
   1. Iterate through every event file in `event_data` to copy their data to a single CSV file- `event_datafile_new.csv` (new CSV file) in Python.
   2. Processing the `event_datafile_new.csv` dataset to create a denormalized dataset.
   3. Model the data tables keeping in mind the `queries` need to run.
   4. `Queries` are provided in the following section to model data tables for.
   5. Load the data into tables, created in Apache Cassandra and run your queries.
   
##### UERIES for which data has to be modeled
   1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and     itemInSession = 4
   2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
   3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
  
##### Data Modeling

New CSV file- `event_datafile_new.csv` created by processing the `events_data` is used to populate denormalized cassandra tables which are optimized for given queries.

 1. `songplays_info` table is to retrieve artist,song tiltle and its length for the given `sessionId` and `itemInSession`
 2. `user_songplays_info` table is to retieve artist, song and user name for the given `userId` and `sessionId`
 3. `user_details` table for retrieving user names (first and last) listening to a particular `song`
 
 The example queries are returned as pandas dataframes to facilitate further data manipulation.


 
 
