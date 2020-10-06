import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType, DateType
from pyspark.sql.functions import udf, col, from_unixtime, to_timestamp
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark




# path to full logs - song_data/*/*/*/*.json
# SONG_DATA = s3a://udacity-dend/song_data/*/*/*/*.json

"""
 |-- artist_id: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_longitude: double (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- duration: double (nullable = true)
 |-- num_songs: long (nullable = true)
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- year: long (nullable = true)
 """



def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'

    # read song data file
    df = spark.read.json(path = song_data, multiLine = True)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet('songs_table.parquet', 'overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude')

    # write artists table to parquet files
    artists_table.write.parquet('artists_table.parquet', 'overwrite')



# LOG_DATA=s3a://udacity-dend/log-data/*.json

"""
 |-- artist: string (nullable = true)
 |-- auth: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- itemInSession: long (nullable = true)
 |-- lastName: string (nullable = true)
 |-- length: double (nullable = true)
 |-- level: string (nullable = true)
 |-- location: string (nullable = true)
 |-- method: string (nullable = true)
 |-- page: string (nullable = true)
 |-- registration: double (nullable = true)
 |-- sessionId: long (nullable = true)
 |-- song: string (nullable = true)
 |-- status: long (nullable = true)
 |-- ts: long (nullable = true)
 |-- userAgent: string (nullable = true)
 |-- userId: string (nullable = true)
"""

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log-data/*/*/*.json')

    # read log data file
    df = spark.read.json(path = log_data, multiLine = True)

    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').drop_duplicates()

    # write users table to parquet files
    users_table.write.parquet('users_table', 'overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ms: datetime.fromtimestamp((ms/1000.0)), TimestampType())
    df = df.withColumn('start_time', get_timestamp(df.ts))

    """
    print("Number of rows = {}".format(df.count()))
    df.printSchema()
    df.select('start_time').limit(10).show()
    df.limit(5).show()
    """

    # create datetime column from original timestamp column
    get_datetime = udf(lambda ms: datetime.fromtimestamp((ms/1000.0)), DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))

    # start_time, hour, day, week, month, year, weekday
    # extract columns to create time table
    time_table = df.select('ts', 'start_time', 'datetime', year(df.datetime).alias('year'), month(df.datetime).alias('month')).drop_duplicates()

    time_table.printSchema()
    time_table.limit(5).show()
    print("Number of rows in time_table = {}".format(time_table.count()))

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet('time_table.parquet', 'overwrite')

    # song_id, artist_id, artist_location from songs_table
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    # read in song data to use for songplays table



    # read song data file
    songs_data_local_path = './songs_table/*/*/*'
    song_df = spark.read.json(path = songs_data_local_path, multiLine = True)
    song_df.printSchema()

    # extract columns from joined song and log datasets to create songplays table
    #songplays_table =

    # write songplays table to parquet files partitioned by year and month
    #songplays_table


def main():
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = ""# PUT THE S3 address here

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
