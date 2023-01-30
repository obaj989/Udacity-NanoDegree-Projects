import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    creates and returns the spark session 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    creates artists and songs table after reading song data 
    """
    
    # get filepath to song data file
    song_data = f"{input_data}song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id','title','artist_id','year','duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy(['year','artist_id']).parquet(f"{output_data}/songs.parquet", mode="overwrite")

    # extract columns to create artists table
    artists_table = df.selectExpr("artist_id",
                                  "artist_name as name",
                                  "artist_location as location",
                                  "artist_latitude as latitude",
                                  "artist_longitude as longitude")
    
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists.parquet", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    """
    creates users, time and song_plays table after reading log data 
    """
    
    # get filepath to log data file
    log_data = f"{input_data}log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr('userId as user_id','firstName as first_name','lastName as last_name','gender','level').distinct()
 
    
    # write users table to parquet files
    users_table.write.parquet("{}/users.parquet".format(output_data), mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ts: datetime.fromtimestamp(ts / 1000.0))
    df = df.withColumn("timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda ts: datetime.fromtimestamp(ts / 1000.0).strftime('%Y-%m-%d %H:%M:%S'))
    df = df.withColumn("datetime", get_datetime("ts"))
    
    # extract columns to create time table
    df.createOrReplaceTempView("log_df")
    time_table = spark.sql("""
    SELECT  DISTINCT datetime AS start_time, 
                     hour(timestamp) AS hour, 
                     day(timestamp)  AS day, 
                     weekofyear(timestamp) AS week,
                     month(timestamp) AS month,
                     year(timestamp) AS year,
                     dayofweek(timestamp) AS weekday
    FROM log_df
    ORDER BY start_time
    """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy(['year','month']).parquet(time_table_path)

    # extract columns from joined song and log datasets to create songplays table
    df_joined = df_log_data.join(df, (df_log_data.artist == df.artist_name) & (df_log_data.song == df.title))
    df_joined = df_joined.withColumn("songplay_id", monotonically_increasing_id())

    df_joined.createOrReplaceTempView("songplays")
    songplays_table = spark.sql("""
    SELECT  songplay_id, 
            timestamp   AS start_time, 
            userId      AS user_id, 
            level,
            song_id,
            artist_id,
            sessionId   AS session_id,
            location,
            userAgent   AS user_agent
    FROM songplays
    ORDER BY (user_id, session_id) 
    """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy(['year','month']).parquet("{}/songplays.parquet".format(output_data), mode="overwrite")



def main():
    """
    call methods to create spark session, process song data and then process log data 
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-dend-sparkproject4-osama/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
