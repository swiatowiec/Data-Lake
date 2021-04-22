import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    
    """
        This function loads song_data from S3 and processes it by extracting the songs and artist tables and then again loaded back to S3
    """
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    song_schema = StructType([
        SFld("artist_id", Str()),
        SFld("artist_latitude", Dbl()),
        SFld("artist_location", Str()),
        SFld("artist_longitude", Dbl()),
        SFld("artist_name", Str()),
        SFld("duration", Dbl()),
        SFld("num_songs", Int()),
        SFld("title", Str()),
        SFld("year", Int()),
       ])
    
    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_f = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(song_fields).dropDuplicates().withColumn("song_id", monotonically_increasing_id())
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(output_data + "songs")


    # extract columns to create artists table
    artists_f = ["artist_id", "artist_name as name", "artist_location as location", "artist_latitude as latitude", "artist_longitude as longitude"]
    artists_table = df.selectExpr(artists_fields).dropDuplicates()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    
    """
    This function loads log_data from S3 and processes it by extracting the songs and artist tables and then again loaded back to S3. Also output from previous function is used in by   spark.read.json command
    """
    
    # get filepath to log data file
    log_data = os.path.join(input_data, 'log_data/*/*/*.json')

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == 'NextSong')

    # extract columns for users table    
    users_f = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_f).dropDuplicates()
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df_log = df_log.withColumn("start_time", get_datetime(df_log.timestamp)) 
    
    # extract columns to create time table
    df_log = df_log.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))

    time_table = df_log.select("start_time", "hour", "day", "week", "month", "year", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data + "time")

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, "songs/*/*/*"))
    songs_logs = df_log.join(songs_df, (df_log.song == songs_df.title))

    # extract columns from joined song and log datasets to create songplays table 
    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))
    artists_songs_logs = songs_logs.join(artists_df, (songs_logs.artist == artists_df.name))
    songplays = artists_songs_logs.join(
        time_table,
        artists_songs_logs.ts == time_table.ts, 'left'
    ).drop(artists_songs_logs.year)

    # write songplays table to parquet files partitioned by year and month
    songplays_table = songplays.select(
        col('start_time'),
        col('userId').alias('user_id'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId').alias('session_id'),
        col('location'),
        col('userAgent').alias('user_agent'),
        col('year'),
        col('month'),
    ).repartition("year", "month")

    songplays_table.write.mode("overwrite").partitionBy("year", "month").parquet(output_data, 'songplays')
                               
def main():
    
    """
    Extract songs and events data from S3, Transform it into dimensional tables format, and Load it back to S3 in Parquet format
    """
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
