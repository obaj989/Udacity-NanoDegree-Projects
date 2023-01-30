# Data Lake for Sparkify

## Overview

The project requires us to build an ETL pipeline using apache spark to extract and store data on Amazon S3. 
Our goal is to increase the analytics speed on sparkify data. We are using the star schema to store data. Data resides in JSON format in S3 and is stored in parquet files
on S3.

## Structure

The project contains the following components:

* `etl.py` reads data from S3, processes it into analytics tables, and then writes them to S3
* `dl.cfg` contains AWS credentials 
* `test.ipnyb` to readily run and test queries

## Analytics Schema

The analytics tables centers on the following fact table:

* `songplays` - which contains a log of user song plays. this is the only fact table.
* `users` - contains the users information
* `songs`- contains the songs information
* `artists`- contains the artist information
* `time`- contains the song time information

## Instructions

To execute the ETL pipeline from the command line, enter the following:

```
python etl.py
```