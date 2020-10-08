"""

This file is an etl script, extracting json files
from a s3 bucket, transforming the data in Spark
to fact and dimension tables, loads them into
another directory, and thus creating a Data Lake.

"""


import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType
from pyspark.sql.functions import udf, col, from_unixtime, to_timestamp, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


# Comment these out if running on EMR cluster
"""
config = configparser.ConfigParser()
config.read('dl.cfg')
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
"""

def create_spark_session():
    """
    Creates a SparkSession

    ARGUMENTS:
        None

    RETURNS:
        spark
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Reads .json files from a s3 bucket, creates songs
    and artists dimension tables, and writes them
    to parquet files.

    ARGUMENTS:
        spark - a SparkSession
        input_data - path to a s3 bucket
        output_data - path to a s3 bucket

    RETURNS:
        None
    """

    # get filepath to song data file
    print('Extract song_data files from s3...')
    song_data = input_data + 'song_data/*/*/*/*.json'  # A/A/A   */*/*


    # read song data file
    df = spark.read.json(path = song_data, multiLine = True)
    print('Extract song_data files from s3    {}'.format('Finished'))


    # extract columns to create songs table
    songs_table = df.select('song_id', \
                            'title', \
                            'artist_id', \
                            'year', \
                            'duration')\
                            .drop_duplicates()


    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + 'songs', 'overwrite')
    print('Writing songs parquet files        {}'.format('Finished'))


    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id as artist', \
                                  'artist_name as name', \
                                  'artist_location as location', \
                                  'artist_latitude as latitude', \
                                  'artist_longitude as longitude')\
                                  .drop_duplicates()


    # write artists table to parquet files
    artists_table.write.parquet(output_data + 'artists', 'overwrite')
    print('Writing artists parquet files      {}'.format('Finished'))




def process_log_data(spark, input_data, output_data):
    """
    Reads .json files from a s3 bucket, creates users
    and time dimension tables, and writes them
    to parquet files. In addition it creats the fact
    table songplays, and writes it to parquet files.

    ARGUMENTS:
        spark - a SparkSession
        input_data - path to a s3 bucket
        output_data - path to a s3 bucket

    RETURNS:
        None
    """

    # get filepath to log data file
    print('Extract log_data files from s3...')
    log_data = os.path.join(input_data + 'log-data/*/*/*.json') # 2018-11-04-events


    # read log data file
    log_df = spark.read.json(path = log_data, multiLine = True)
    print('Extract log_data files from s3     {}'.format('Finished'))


    # filter by actions for song plays
    log_df = log_df.where(log_df.page == 'NextSong')


    # extract columns for users table
    users_table = log_df.select('userId', \
                                'firstName', \
                                'lastName', \
                                'gender', \
                                'level')\
                                .drop_duplicates()\
                                .where(col("userId").isNotNull())


    # write users table to parquet files
    users_table.write.parquet(output_data + 'users', 'overwrite')
    print('Writing users parquet files        {}'.format('Finished'))


    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ms: datetime.fromtimestamp((ms/1000.0)), TimestampType())
    log_df_time = log_df.withColumn('start_time', get_timestamp(log_df.ts))


    # extract columns to create time table
    time_table = log_df_time.select('start_time').drop_duplicates()

    time_table = time_table.select('start_time', \
                                    hour('start_time').alias('hour'), \
                                    dayofmonth('start_time').alias('day'), \
                                    weekofyear('start_time').alias('week'), \
                                    month('start_time').alias('month'), \
                                    year('start_time').alias('year'), \
                                    date_format('start_time', 'u').alias('weekday'))


    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet(output_data + 'time', 'overwrite')
    print('Writing time parquet files         {}'.format('Finished'))


    # read in song data to use for songplays table
    song_df = spark.read\
                .format("parquet")\
                .option("basePath", 'songs/')\
                .load('songs/*/*/*.parquet')

    artist_df = spark.read\
                .format("parquet")\
                .option("basePath", 'artists/')\
                .load('artists/*.parquet')


    # read song_and_artists data file
    song_and_artists = song_df.join(artist_df, (song_df.artist_id == artist_df.artist)) \
                                    .select('song_id', \
                                            'title', \
                                            'artist_id', \
                                            'year', \
                                            'duration', \
                                            artist_df.location, \
                                            artist_df.name)


    # extract columns from joined song and log datasets to create songplays table
    songplays_table = song_and_artists.join(log_df_time, (song_and_artists.title == log_df_time.song) & \
                                                         (song_and_artists.name == log_df_time.artist) & \
                                                         (song_and_artists.duration == log_df_time.length), \
                                                         ('left_outer')) \
                                            .select(monotonically_increasing_id().alias('songplay_id'), \
                                                   log_df_time.start_time, \
                                                   log_df_time.userId.alias('user_id'), \
                                                   log_df.level, \
                                                   'song_id', \
                                                   'artist_id', \
                                                   log_df.sessionId.alias('session_id'), \
                                                   song_and_artists.location, \
                                                   log_df.userAgent.alias('user_agent'), \
                                                   year(log_df_time.start_time).alias('year'), \
                                                   month(log_df_time.start_time).alias('month'))


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet(output_data + 'songplays', 'overwrite')
    print('Writing songplays parquet files    {}'.format('Finished'))




def main():
    """
    Creates a SparkSession
    Sets the input_data to the s3 directory
    Sets the output_data to the s3 directory
    Runs the 'process_song_data' function
    Runs the 'process_log_data' function

    ARGUMENTS:
        None

    RETURNS:
        None
    """

    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalakebucketudacity/"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    print('\nJob done!\n')




if __name__ == "__main__":
    main()
