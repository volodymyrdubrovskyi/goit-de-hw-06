import pandas as pd
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, round, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell'

# Створення сесії Spark
spark = SparkSession.builder \
    .appName("Kafka Spark Streaming") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.cores", "1") \
    .config("spark.driver.cores", "1") \
    .config("spark.task.cpus", "1") \
    .config("spark.default.parallelism", "1") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "4") \
    .config("spark.dynamicAllocation.maxExecutors", "40") \
    .config("spark.dynamicAllocation.initialExecutors", "10") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
    .getOrCreate()

# Структура даних сенсора
schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType())
])

# Зчитування даних з Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
    .option("subscribe", "vvd_building_sensors") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", "50") \
    .option("failOnDataLoss", "false") \
    .load()

# Декодування та парсинг даних
df = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

def print_to_console_0(df, epoch_id):
    if df.count() > 0:
        print("df:")
        sorted_df = df.orderBy(col("timestamp").asc()) 
        sorted_df.show(truncate=False, n=100)

df.writeStream \
  .trigger(availableNow=True) \
  .outputMode("append") \
  .format("console") \
  .foreachBatch(print_to_console_0) \
  .start() \
  .awaitTermination()

# Застосування Sliding Window
windowed_avg = df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(round(avg("temperature"), 1).alias("t_avg"), round(avg("humidity"), 1).alias("h_avg"))

def print_to_console_1(df, epoch_id): 
    if df.count() > 0:
        print("windowed_avg:")
        sorted_df = df.orderBy(col("window").asc()) 
        sorted_df.show(truncate=False, n=100)

query1 = windowed_avg.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(print_to_console_1) \
    .start() \
    .awaitTermination()

alerts_df = pd.read_csv("alerts_conditions.csv")
alerts_spark_df = spark.createDataFrame(alerts_df)

alerts_crossed = windowed_avg.crossJoin(alerts_spark_df)

alerts_filtered = alerts_crossed.filter(
    ( (alerts_crossed['humidity_min'] == -999) | (alerts_crossed['h_avg'] >= alerts_crossed['humidity_min']) ) &
    ( (alerts_crossed['humidity_max'] == -999) | (alerts_crossed['h_avg'] <= alerts_crossed['humidity_max']) ) &
    ( (alerts_crossed['temperature_min'] == -999) | (alerts_crossed['t_avg'] >= alerts_crossed['temperature_min']) ) &
    ( (alerts_crossed['temperature_max'] == -999) | (alerts_crossed['t_avg'] <= alerts_crossed['temperature_max']) ) 
)

alerts_filtered_columns = alerts_filtered.select( 
    col("window"), 
    col("t_avg"), 
    col("h_avg"), 
    col("code"), 
    col("message") 
)

def print_to_console_2(df, epoch_id): 
    if df.count() > 0:
        print("alerts_filtered_columns:")
        sorted_df = df.orderBy(col("window").asc()) 
        sorted_df.show(truncate=False, n=100)

alerts_filtered_columns = alerts_filtered_columns.withColumn("code", col("code").cast("string"))

query2 = alerts_filtered_columns.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(print_to_console_2) \
    .start() \
    .awaitTermination()

alerts_filtered_columns.printSchema()

# current_dir = os.path.dirname(os.path.abspath(__file__))
# checkpoint_dir = os.path.join(current_dir, "checkpoints")

# Створення JSON з поточних стовпчиків
json_df = alerts_filtered_columns.select(to_json(struct("window", "t_avg", "h_avg", "code", "message")).alias("value"))

def print_to_console_and_kafka(df, epoch_id):
    if df.count() > 0:
        print("JSON:")
        df.show(truncate=False, n=100)
        
        # Запис стріму у Kafka з перевіркою на консолі 
        df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "77.81.230.104:9092") \
            .option("kafka.security.protocol", "SASL_PLAINTEXT") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="VawEzo1ikLtrA8Ug8THa";') \
            .option("topic", "vvd_building_sensors_alerts") \
            .save()
    else:
        print("JSON empty")

# Перевірка вмісту json_df
query5 = json_df.writeStream \
    .trigger(availableNow=True) \
    .format("console") \
    .outputMode("append") \
    .foreachBatch(print_to_console_and_kafka) \
    .start()
    
query5.awaitTermination()