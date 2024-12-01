import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from configs import kafka_config
from confluent_kafka import Producer

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
    .option("failOnDataLoss", "false") \
    .load()

# Декодування та парсинг даних
df = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Застосування Sliding Window
windowed_avg = df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds")) \
    .agg(round(avg("temperature"), 1).alias("t_avg"), round(avg("humidity"), 1).alias("h_avg"))

def print_to_console_1(df, epoch_id): 
    # Сортування даних всередині функції foreachBatch перед виведенням 
    sorted_df = df.orderBy(col("window").asc()) 
    sorted_df.show(truncate=False, n=100)

# Виведення результатів на консоль
query1 = windowed_avg.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .foreachBatch(print_to_console_1) \
    .start()

# Зчитування умов алертів з CSV файлу 
alerts_df = pd.read_csv("alerts_conditions.csv")
alerts_spark_df = spark.createDataFrame(alerts_df)

# Застосування cross join з умовами алертів 
alerts_crossed = windowed_avg.crossJoin(alerts_spark_df)

# Фільтрація на основі умов алертів 
alerts_filtered = alerts_crossed.filter(
    ( (alerts_crossed['humidity_min'] == -999) | (alerts_crossed['h_avg'] >= alerts_crossed['humidity_min']) ) &
    ( (alerts_crossed['humidity_max'] == -999) | (alerts_crossed['h_avg'] <= alerts_crossed['humidity_max']) ) &
    ( (alerts_crossed['temperature_min'] == -999) | (alerts_crossed['t_avg'] >= alerts_crossed['temperature_min']) ) &
    ( (alerts_crossed['temperature_max'] == -999) | (alerts_crossed['t_avg'] <= alerts_crossed['temperature_max']) ) 
)

# Вибір необхідних стовпчиків з alerts_filtered 
alerts_filtered_columns = alerts_filtered.select( 
    col("window"), 
    col("t_avg"), 
    col("h_avg"), 
    col("code"), 
    col("message") 
)

def print_to_console_2(df, epoch_id): 
    # Сортування даних всередині функції foreachBatch перед виведенням 
    sorted_df = df.orderBy(col("window").asc()) 
    sorted_df.show(truncate=False, n=100)

# Виведення результатів на консоль 
query2 = alerts_filtered_columns.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .foreachBatch(print_to_console_2) \
    .start()

# Створення продюсера Kafka 
producer = Producer(kafka_config)

# Функція для відправки даних у Kafka 
def send_to_kafka(df, epoch_id): 
    df = df.selectExpr("to_json(struct(*)) AS value") 
    try:
        for row in df.collect(): 
            producer.produce("vvd_building_sensors_alerts", value=row["value"]) 
        print("data sent sucsessfully...")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.flush()

# Запис даних в топік vvd_building_sensors_alerts 
query3 = alerts_filtered_columns.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .foreachBatch(send_to_kafka) \
    .start()

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()