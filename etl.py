import configparser
from datetime import datetime
import os
import timeit
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, TimestampType, DateType
from pyspark.sql.functions import udf, col, from_unixtime, to_timestamp, monotonically_increasing_id
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


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json' #  */*/*   A/A/A

    # read song data file
    df = spark.read.json(path = song_data, multiLine = True)
    print('Loaded and read song_data files from s3')


    # extract columns to create songs table
    songs_table = df.select('song_id', \
                            'title', \
                            'artist_id', \
                            'year', \
                            'duration')\
                            .drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet('songs', 'overwrite')
    print('Wrote songs to parquet locally')
    #print("Number of rows in songs_table = {}".format(songs_table.count()))
    #print('songs_table: ')
    #songs_table.printSchema()
    #songs_table.limit(5).show()

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id as artist', \
                                  'artist_name as name', \
                                  'artist_location as location', \
                                  'artist_latitude as latitude', \
                                  'artist_longitude as longitude')\
                                  .drop_duplicates()


    # write artists table to parquet files
    artists_table.write.parquet('artists', 'overwrite')
    print('Wrote artists to parquet locally')
    #print("Number of rows in artists_table = {}".format(artists_table.count()))
    #print('artists_table: ')
    #artists_table.printSchema()
    #artists_table.limit(5).show()



def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log-data/*/*/*.json') # 'log-data/*/*/2018-11-04-events.json'

    # read log data file
    log_df = spark.read.json(path = log_data, multiLine = True)
    print('Loaded and read log_data files from s3')

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
    users_table.write.parquet('users', 'overwrite')
    print('Wrote users to parquet locally')
    #print("Number of rows in users_table = {}".format(users_table.count()))
    #print('users_table: ')
    #users_table.printSchema()
    #users_table.limit(5).show()

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda ms: datetime.fromtimestamp((ms/1000.0)), TimestampType())
    log_df_time = log_df.withColumn('start_time', get_timestamp(log_df.ts))

    # create datetime column from original timestamp column
    # get_datetime = udf(lambda ms: datetime.fromtimestamp((ms/1000.0)), DateType())
    # df = df.withColumn('datetime', get_datetime(df.ts))

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
    time_table.write.partitionBy('year', 'month').parquet('time', 'overwrite')
    print('Wrote time to parquet locally')
    #print("Number of rows in time_table = {}".format(time_table.count()))
    #print('time_table: ')
    #time_table.printSchema()
    #time_table.limit(5).show()

    # get (song_id, artist_id, artist_location) from songs_table
    # get (songplay_id, start_time, user_id, level, session_id, user_agent) from log_table
    # read in song data to use for songplays table

    song_df = spark.read\
                .format("parquet")\
                .option("basePath", 'songs/')\
                .load('songs/*/*/*.parquet')

    artist_df = spark.read\
                .format("parquet")\
                .option("basePath", 'artists/')\
                .load('artists/*.parquet')


    song_and_artists = song_df.join(artist_df, (song_df.artist_id == artist_df.artist)) \
                                    .select('song_id', \
                                            'title', \
                                            'artist_id', \
                                            'year', \
                                            'duration', \
                                            artist_df.location, \
                                            artist_df.name)
    """
    print('Read locally from songs_table and artists_table')
    print("Number of rows in song_df = {}".format(song_df.count()))
    print('song_df: ')
    song_df.printSchema()
    song_df.limit(5).show()
    print("Number of rows in artist_df = {}".format(artist_df.count()))
    print('artist_df: ')
    artist_df.printSchema()
    artist_df.limit(5).show()
    print("Number of rows in song_and_artists = {}".format(song_and_artists.count()))
    print('song_and_artists: ')
    song_and_artists.printSchema()
    song_and_artists.limit(5).show()
    """

    # read song data file
    # songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent


    #
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

    #print("Number of rows in songplays_table = {}".format(songplays_table.count()))
    #print('songplays_table: ')
    #songplays_table.printSchema()
    #songplays_table.limit(20).show()


    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month').parquet('songplays', 'overwrite')
    print('Wrote songplays to parquet locally')

def main():
    spark = create_spark_session()

    input_data = "s3a://udacity-dend/"
    output_data = ""# PUT THE S3 address here

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

    print('\nJob done!\n')


if __name__ == "__main__":
    main()
