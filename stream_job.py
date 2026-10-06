import argparse
from pyspark.sql import SparkSession, functions as F, types as T

S = T.StringType()
TICKER_SCHEMA = T.StructType([
    T.StructField("type", S), T.StructField("sequence", T.LongType()),
    T.StructField("product_id", S), T.StructField("price", S),
    T.StructField("best_bid", S), T.StructField("best_ask", S),
    T.StructField("last_size", S), T.StructField("side", S),
    T.StructField("time", S), T.StructField("trade_id", T.LongType()),
])

def main(a):
    spark = (SparkSession.builder.appName("coinbase-stream").master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    # 1. Ingest: read lines from the bridge
    raw = (spark.readStream.format("socket")
           .option("host", a.host).option("port", a.port).load())

    # 2. Parse and clean: JSON string -> typed columns, drop non-ticker and bad rows
    ticks = (
        raw.select(F.from_json("value", TICKER_SCHEMA).alias("m"))
           .where(F.col("m.type") == "ticker")
           .select("m.*")
           .withColumn("event_time", F.to_timestamp("time"))
           .withColumn("price", F.col("price").cast("double"))
           .withColumn("best_bid", F.col("best_bid").cast("double"))
           .withColumn("best_ask", F.col("best_ask").cast("double"))
           .withColumn("last_size", F.col("last_size").cast("double"))
           .withColumn("spread", F.col("best_ask") - F.col("best_bid"))
           .where(F.col("event_time").isNotNull() & F.col("price").isNotNull())
           .drop("time", "type")
    )

    # 3. Analyse: tumbling-window stats per product
    stats = (
        ticks.withWatermark("event_time", "30 seconds")
             .groupBy(F.window("event_time", a.window), "product_id")
             .agg(
                 F.count(F.lit(1)).alias("ticks"),
                 F.min_by("price", "event_time").alias("open"),
                 F.max_by("price", "event_time").alias("close"),
                 F.max("price").alias("high"),
                 F.min("price").alias("low"),
                 F.sum("last_size").alias("volume"),
                 (F.sum(F.col("price") * F.col("last_size")) / F.sum("last_size")).alias("vwap"),
                 F.avg("spread").alias("avg_spread"),
             )
             .withColumn("return_pct", (F.col("close") - F.col("open")) / F.col("open") * 100)
             .select(F.col("window.start").alias("window_start"),
                     F.col("window.end").alias("window_end"),
                     "product_id", "ticks", "open", "close", "high", "low",
                     "volume", "vwap", "avg_spread", "return_pct")
    )

    # 4. Output
    if a.sink == "console":
        q = (stats.writeStream.outputMode("update").format("console")
             .option("truncate", False).trigger(processingTime="10 seconds").start())
    else:  # parquet requires append mode: a window is written after the watermark passes it
        q = (stats.writeStream.outputMode("append").format("parquet")
             .option("path", "out/stats").option("checkpointLocation", "out/_chk")
             .trigger(processingTime="10 seconds").start())
    q.awaitTermination()

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=9999)
    p.add_argument("--window", default="10 seconds")
    p.add_argument("--sink", choices=["console", "parquet"], default="console")
    main(p.parse_args())