from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, window, avg, round, to_json, struct


# Створення сесії Spark
spark = SparkSession.builder \
    .appName("Kafka Spark Streaming") \
    .config("spark.executor.memory", "12g") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.cores", "6") \
    .config("spark.driver.cores", "2") \
    .config("spark.task.cpus", "1") \
    .config("spark.default.parallelism", "2") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.dynamicAllocation.minExecutors", "4") \
    .config("spark.dynamicAllocation.maxExecutors", "40") \
    .config("spark.dynamicAllocation.initialExecutors", "10") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1") \
    .config("spark.executorEnv.PYSPARK_PYTHON", "python") \
    .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", "python") \
    .config("spark.pyspark.python", "python") \
    .config("spark.pyspark.driver.python", "python") \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:log4j.properties") \
    .getOrCreate()
    
# Структура даних сенсора
schema = StructType([
    StructField("sensor_id", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType())
])

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
    .option("maxOffsetsPerTrigger", "5") \
    .load()
    
# Декодування та парсинг даних
df = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")
    
def print_to_console_1(df, epoch_id): 
    # Сортування даних всередині функції foreachBatch перед виведенням 
    sorted_df = df.orderBy(col("timestamp").asc())
    total_count = sorted_df.count() 
    print(f"Total number of rows: {total_count}")
    sorted_df.show(truncate=False, n=30)

# Виведення результатів на консоль
query1 = df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(print_to_console_1) \
    .start() \
    .awaitTermination()