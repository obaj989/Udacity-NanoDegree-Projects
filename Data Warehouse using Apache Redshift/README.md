# Data Warehouse Project Using Redshift and AWS

## Project Overview
We are provided with the JSON logs and songs metadata for a music streaming app "sparkify". Sparkify's analytics team wants us to 
develop an ETL pipeline to extract the data from an S3 bucket, stage it in Amazon Redshift and then build a start schema for them 
to find insight to which songs users are listening to.

## Project Structure

Project cosists of the following files:

* `create_tables.py` to create the staging tables and start schema tables
* `dwh.cfg` contains database credentials and ARN value of the cluster alongwith the data paths on S3
* `etl.py` to extract data from S3, copy it to staging tables and perform data insertion queries for star schema tables
* `sql_queries.py` defines SQL queries for creating/dropping tables, copying data from S3 and populating our fact and dimension tables
* `test.ipynb` to perform some exploratory analysis on the data and run the files

## Project Instructions

First thing is to create a cluster with IAM role and add these details in the configuration file. Make sure that
the cluster is running before proceeding further.
To execute the ETL on an existing cluster from the command line, enter the
following:

```
python3 create_tables.py
python3 etl.py
```

## Sample Query
We can now run a query to extract which users are listening to which songs:

```
SELECT 
    s.songplay_id, 
    s.start_time,
    s.user_id,
    u.first_name,
    u.last_name,
    u.gender,
    sn.title,
    sn.artist_id
FROM songplays s JOIN songs sn ON s.song_id = sn.song_id
JOIN users u ON s.user_id = u.user_id LIMIT 5
```
