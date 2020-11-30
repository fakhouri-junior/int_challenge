from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

loc_weather_data = "data/2019/"
country_data_name = "countrylist.csv"
station_data_name = "stationlist.csv"

saving_path = "data/parquet/df_parquet"

def is_dir_empty(spath):
    if os.path.exists(spath):
        if len(os.listdir(spath)) == 0:
            return True
        else:
            return False
    else:
        return True


def read_csv_spark(spark, file_location):
    df = (spark.read.format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .load(file_location)
                  )
    return df

def get_data(spark):
    if is_dir_empty(saving_path):
        print("directory is empty, creating data ...")
        weather_df = read_csv_spark(spark, loc_weather_data)
        country_df = read_csv_spark(spark, country_data_name)
        station_df = read_csv_spark(spark, station_data_name)

        """
        join country with station
        """
        # avoid ambiguity
        weather_df = weather_df.withColumnRenamed("STN---", "STATION_NUMBER")
        station_df_renamed = station_df.withColumnRenamed("COUNTRY_ABBR", "COUNTRY_ABBR_2")

        country_station = country_df.join(station_df_renamed, country_df.COUNTRY_ABBR == station_df_renamed.COUNTRY_ABBR_2)\
            .select("COUNTRY_ABBR", "COUNTRY_FULL", "STN_NO")


        df_full = country_station.join(weather_df, weather_df.STATION_NUMBER == country_station.STN_NO)

        # save data for faster iterations
        (df_full.write.format("parquet")
          .option("compression", "snappy")
          .save(saving_path))

    else:
        print("directory is not empty, fetching saved files ...")
        df_full = spark.read.format("parquet").load("data/parquet/df_parquet")

    return df_full
