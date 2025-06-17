from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType

spark = (SparkSession.builder
         .appName("KafkaStreamConsumer")
         .getOrCreate())

json_schema = StructType([
    StructField("event_time", TimestampType(), True),
    StructField("device_id",  StringType(),   True),
    StructField("metric",     StringType(),   True),
    StructField("value",      DoubleType(),   True),
])

kafka_df = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "rc1a-edrhu2fdhseflv91.mdb.yandexcloud.net:9091")
            .option("subscribe", "smart_home_events")
            .option("startingOffsets", "latest")
            .option("kafka.security.protocol", "SASL_SSL")
            .option("kafka.sasl.mechanism",    "SCRAM-SHA-512")
            .option("kafka.sasl.jaas.config",
                    'org.apache.kafka.common.security.scram.ScramLoginModule '
                    'required username="smart_home_events" password="smart_home_events";')
            .load())

parsed_df = (kafka_df
             .selectExpr("CAST(value AS STRING) AS json_str")
             .select(from_json(col("json_str"), json_schema).alias("data"))
             .select("data.*"))

agg_df = (parsed_df
          .withWatermark("event_time", "2 minutes")
          .groupBy(
               window(col("event_time"), "1 minute").alias("win"),
               col("metric")
          )
          .agg(
               count("*").alias("cnt"),
               avg("value").alias("avg_value")
          )
          .select(
               col("win.start").alias("window_start"),
               col("win.end").alias("window_end"),
               "metric", "cnt", "avg_value"
          ))

(agg_df.writeStream
      .outputMode("append")
      .format("csv")
      .option("header", "true")
      .option("path", "s3a://data-proc-bckt/stream_out")
      .option("checkpointLocation",
              "s3a://data-proc-bckt/checkpoints/kafka_consumer")
      .trigger(processingTime="1 minute")
      .start()
      .awaitTermination())