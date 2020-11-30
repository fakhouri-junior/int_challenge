from pyspark.sql import SparkSession
from load_data import get_data
from queries import cons_days_torn_funnel, get_average_over_year

spark = (SparkSession
         .builder
         .appName("Challenge")
         .getOrCreate()
         )

df_full = get_data(spark)


# q1
ans1 = get_average_over_year(df_full, desired_column="TEMP", missing_value_default=9999.9, take=1)
print("First Answer {}".format(ans1))

# Q2
ans2 = cons_days_torn_funnel(df_full)
print("Second Answer {}".format(ans2.show(n=1, truncate=False)))


# Q3
ans3 = get_average_over_year(df_full, desired_column="WDSP", missing_value_default=999.9, take=2)
print("Third Answer {}".format(ans3))





