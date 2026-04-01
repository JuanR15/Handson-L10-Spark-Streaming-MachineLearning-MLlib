from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, hour, minute
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

TRAINING_DATA_PATH = "training-dataset.csv"

spark = SparkSession.builder \
    .appName("Task5_FareTrendPrediction") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("[Training] Training trend model...")

hist_df = spark.read.csv(TRAINING_DATA_PATH, header=True, inferSchema=True) \
    .withColumn("event_time", col("timestamp").cast(TimestampType())) \
    .withColumn("fare_amount", col("fare_amount").cast(DoubleType()))

hist_windowed = hist_df.groupBy(
    window(col("event_time"), "5 minutes")
).agg(
    avg("fare_amount").alias("avg_fare")
)

hist_features = hist_windowed \
    .withColumn("hour_of_day", hour(col("window.start")).cast(DoubleType())) \
    .withColumn("minute_of_hour", minute(col("window.start")).cast(DoubleType()))

assembler = VectorAssembler(
    inputCols=["hour_of_day", "minute_of_hour"],
    outputCol="features"
)

train_df = assembler.transform(hist_features)

lr = LinearRegression(featuresCol="features", labelCol="avg_fare")
trend_model = lr.fit(train_df)

print("[Training Complete] Trend model trained in memory.")

schema = StructType([
    StructField("trip_id", IntegerType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

raw_stream = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

parsed_stream = raw_stream.select(
    from_json(col("value"), schema).alias("data")
).select("data.*").withColumn(
    "event_time",
    col("timestamp").cast(TimestampType())
)

windowed_df = parsed_stream \
    .withWatermark("event_time", "5 seconds") \
    .groupBy(window(col("event_time"), "20 seconds", "10 seconds")) \
    .agg(avg("fare_amount").alias("avg_fare"))

windowed_features = windowed_df \
    .withColumn("hour_of_day", hour(col("window.start")).cast(DoubleType())) \
    .withColumn("minute_of_hour", minute(col("window.start")).cast(DoubleType()))

feature_df = assembler.transform(windowed_features)

predictions = trend_model.transform(feature_df)

output = predictions.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "avg_fare",
    col("prediction").alias("predicted_next_avg_fare")
)

query = output.writeStream \
    .format("console") \
    .outputMode("update") \
    .option("truncate", False) \
    .start()

query.awaitTermination()