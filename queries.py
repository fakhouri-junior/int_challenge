from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import Window
from udfs import is_tornado_udf, convert_date_udf

def cons_days_torn_funnel(df):
    # add is_tornado column
    df = df.withColumn('is_tornado', is_tornado_udf('FRSHTT'))

    # # filter null values
    df = df.where(col("is_tornado").isNotNull())
    df = df.withColumn('date', convert_date_udf('YEARMODA'))

    df = df.withColumn("on_date", to_timestamp("date", "MM/dd/yyyy"))

    df = df.select("COUNTRY_FULL", "on_date", "is_tornado", "STATION_NUMBER")
    df = df.withColumn("last_event", lag('on_date').over(Window.partitionBy('COUNTRY_FULL').orderBy('on_date')))

    df = df.withColumn('lag_in_day',(unix_timestamp('on_date') - unix_timestamp('last_event')) / (3600 * 24))
    df = df.withColumn('is_consecutive', when(col('lag_in_day') == 1, 1).otherwise(0))

    df = df.withColumn("consecutive_id", sum('is_consecutive').over(
        Window.partitionBy('COUNTRY_FULL').orderBy('on_date')))

    ss = df.where(df["is_consecutive"] != 0)

    ss = ss.groupBy("COUNTRY_FULL", "consecutive_id").count().orderBy("count", ascending=False)
    return ss

def get_average_over_year(df, desired_column, missing_value_default, take=1):
    df = df.withColumn('date', convert_date_udf('YEARMODA'))

    df = df.withColumn("on_date", to_timestamp("date", "MM/dd/yyyy"))
    df_needed = (df.select("COUNTRY_FULL", year("on_date").alias("year"), desired_column)
                 )

    # handle missing_value_default
    clean_column_name = "clean" + desired_column.lower()
    df_needed = df_needed.withColumn(clean_column_name, when(df_needed[desired_column] == missing_value_default, 0).otherwise(df_needed[desired_column]))

    avg_column_name = "avg" + desired_column.lower()
    df_needed = df_needed.groupBy("COUNTRY_FULL", "year").agg(avg(clean_column_name).alias(avg_column_name))\
        .orderBy(avg_column_name, ascending=False)

    return df_needed.take(take)[take-1]