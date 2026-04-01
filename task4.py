import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, abs as abs_diff
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

TRAINING_DATA_PATH = "training-dataset.csv"

spark = SparkSession.builder \
    .appName("Task4_FarePrediction") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("[Training] Training fare model...")

train_df = spark.read.csv(TRAINING_DATA_PATH, header=True, inferSchema=True) \
    .withColumn("distance_km", col("distance_km").cast(DoubleType())) \
    .withColumn("fare_amount", col("fare_amount").cast(DoubleType()))

assembler = VectorAssembler(inputCols=["distance_km"], outputCol="features")
train_data = assembler.transform(train_df)

lr = LinearRegression(featuresCol="features", labelCol="fare_amount")
model = lr.fit(train_data)

print("[Training Complete] Model trained in memory.")

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
).select("data.*")

stream_features = assembler.transform(parsed_stream)

predictions = model.transform(stream_features)

result = predictions.withColumn(
    "deviation",
    abs_diff(col("fare_amount") - col("prediction"))
)

output = result.select(
    "trip_id",
    "driver_id",
    "distance_km",
    "fare_amount",
    col("prediction").alias("predicted_fare"),
    "deviation"
)

query = output.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()