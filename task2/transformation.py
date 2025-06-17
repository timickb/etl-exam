from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

spark = SparkSession.builder.appName("SmartphonesAggregation").getOrCreate()

input_path = "s3a://data-proc-bckt/smartphones.csv"
output_path = "s3a://data-proc-bckt/smartphone_brand_stats.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)

df_clean = df.filter(col("rating").isNotNull())

agg_df = df_clean.groupBy("brand_name").agg(
    count("*").alias("models_count"),
    avg("rating").alias("rating")
)

agg_df.coalesce(1).write \
    .option("header", "true") \
    .mode("overwrite") \
    .csv(output_path)

spark.stop()