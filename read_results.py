from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("read-results").master("local[1]")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet("out/stats")
print("Total window rows:", df.count())

print("--- Latest windows ---")
df.orderBy(F.col("window_start").desc(), "product_id").show(15, truncate=False)

print("--- Summary per product ---")
(df.groupBy("product_id")
   .agg(F.count("*").alias("windows"),
        F.sum("ticks").alias("total_ticks"),
        F.sum("volume").alias("total_volume"),
        F.avg("avg_spread").alias("avg_spread"),
        F.min("low").alias("lowest"),
        F.max("high").alias("highest"))
   .show(truncate=False))
df.toPandas().to_csv("out/stats.csv", index=False)