# PROJECT-1: Data Modelling with Postgres

## Setup Guide

First step is to isntall python, postgresql, psycopg2 library which the postgresql adapter for python and pandas for further processing.

After resolving these dependencies, on the CLI, run:

`python3 create_tables.py`

This will drop any existing tables and create the Postgresql Database.

`python3 etl.py`

This will process the input files and insert data into the database

---

## Overview

A statrtup called Sparkify, which is a music streaming library, wants a Data Engineer to develop a way to analyze the JSON logs and metadata on their songs 
in understanding what songs users are listening to. They'd like to create a Postgres database with tables designed to optimize queries on song play analysis. 
The goal is to create a database schema and ETL pipeline for this analysis. We are provided with the data set in JSON format in two parts:

* **song_data**: The data contains the general info regarding songs and their artists.
* **log_data**: This is the service metadata that contains the real time usage of the music streaming app. This tell us what users are actually doing on the app.

---

## Database Design

Since we are dealing with an analytics problem that is going to be read extensive, our chosen design is a [STAR SCHEMA](https://en.wikipedia.org/wiki/Star_schema)
with a fact table to store all the statistics and real time data, and four dimension tables to store artists, songs, users and the time data. 
Such denormalized schema is going to simplify our anlytics quireies and will enable us to do fast aggregations.

### Fact Table

* **songplays**: real time data of song played, artist and user info 
  * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

* **users**: all information about the user 
  * user_id, first_name, last_name, gender, level
* **songs**: all information about the song 
  * song_id, title, artist_id, year, duration
* **artists**: all information about the artist
  * artist_id, name, location, latitude, longitude
* **time**: detailed breakdown of the time information about the song played
  * start_time, hour, day, week, month, year, weekday

---

## Implementation

### DDL commands

First step is to write the DDL (Data Definition Language) commands to create the tables in the database with the correct constraints.
For convenience and testing, we also write the commands to drop any existing tables. **create_tables.py** will use these queires to
drop any existing tables or database if ti exists and create new tables everytime we run it.

### DML commands
Second step is to write the DML (Data Manipulation Language) commands to inset data into the tables created with conflicts resolution. 
An example of a conflict can be that we are trying to add a user that already exists in the database. These queries will be used in 
**etl.py** to insert the data into the tables

### ETL (Extract, Transorm, Load) Process
Final step is to develop an ETL pipeline to Extract the data from the JSON files, Transform them in a format acceptable by our schema,
and Load them in the database. We perform the following steps here:

* Crawl through the song_data
* Extract the keys related to song info and insert them as a record in the "songs" table
* Extract the keys related to artist info and insert them as a record in the "artists" table
* Crawl through the log_data
* Filter records with the **NextSong** page as these are the records associated with the song plays
* Extract the timestamp, break it down and insert the broken down info as a record in the "time" table
* Extract the keys related to user info and insert them as a record in the "users" table
* Query the song_id and artist_id from their respective tables using the song, artist and duration of the song. 
* Extract the keys related to real time usage and store them as a record along with the song_id and artist_id extracted in the "songplays" table

## Helper Files

* **etl.ipynb**: To write ETL logic step by step and test it at every step. The code written here makes writing etl.py very easy
* **test.ipynb**: To run and test the project at every step. Here we can check the results of our DDL and DML statements and run Sanity checks to make sure that our project is foolproof.
