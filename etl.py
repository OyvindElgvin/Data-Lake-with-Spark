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




def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json' # THIS IS ONLY A SMALL PART OF THE WHOLE SET

    # read song data file
    df = spark.read.json(path = song_data, multiLine = True)
    print('Loaded and read song_data files from s3')

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').drop_duplicates()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet('songs_table', 'overwrite')
    print('Wrote songs_table to parquet locally')
    #print("Number of rows in songs_table = {}".format(songs_table.count()))
    #print('songs_table: ')
    #songs_table.printSchema()
    #songs_table.limit(5).show()

    # extract columns to create artists table
    artists_table = df.selectExpr('artist_id as artist', \
                                  'artist_name as name', \
                                  'artist_location as location', \
                                  'artist_latitude as latitude', \
                                  'artist_longitude as longitude')


    # write artists table to parquet files
    artists_table.write.parquet('artists_table', 'overwrite')
    print('Wrote artists_table to parquet locally')
    #print("Number of rows in artists_table = {}".format(artists_table.count()))
    #print('artists_table: ')
    #artists_table.printSchema()
    #artists_table.limit(5).show()



def process_log_data(spark, input_data, output_data):

    # get filepath to log data file
    log_data = os.path.join(input_data + 'log-data/*/*/*.json')

    # read log data file
    log_df = spark.read.json(path = log_data, multiLine = True)
    print('Loaded and read log_data files from s3')

    # filter by actions for song plays
    log_df = log_df.where(log_df.page == 'NextSong')

    # extract columns for users table
    users_table = log_df.select('userId', 'firstName', 'lastName', 'gender', 'level').drop_duplicates().where(col("userId").isNotNull())

    # write users table to parquet files
    users_table.write.parquet('users_table', 'overwrite')
    print('Wrote users_table to parquet locally')
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
    time_table = log_df_time.select('start_time', hour('start_time').alias('hour'),\
                                               dayofmonth('start_time').alias('day'),\
                                               weekofyear('start_time').alias('week'),\
                                               month('start_time').alias('month'),\
                                               year('start_time').alias('year'),\
                                               date_format('start_time', 'u').alias('weekday'))





    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy('year', 'month').parquet('time_table', 'overwrite')
    print('Wrote time_table to parquet locally')
    #print("Number of rows in time_table = {}".format(time_table.count()))
    #print('time_table: ')
    #time_table.printSchema()
    #time_table.limit(5).show()

    # get (song_id, artist_id, artist_location) from songs_table
    # get (songplay_id, start_time, user_id, level, session_id, user_agent) from log_table
    # read in song data to use for songplays table
    song_df = spark.read.parquet('songs_table/*/*/*.parquet')
    print('Read locally from songs_table into song_df')
    print("Number of rows in song_df = {}".format(song_df.count()))
    print('song_df: ')
    song_df.printSchema()
    song_df.limit(5).show()

    artists_df = spark.read.parquet('artists_table/*.parquet')
    print('Read locally from artists_table into artists_df')
    print("Number of rows in artists_df = {}".format(artists_df.count()))
    print('artists_df: ')
    artists_df.printSchema()
    artists_df.limit(5).show()





    # read song data file


    # extract columns from joined song and log datasets to create songplays table
    #songplays_table = log_df.join(song_df)where()

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
