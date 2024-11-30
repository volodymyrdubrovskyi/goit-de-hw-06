import json 
import time 
import random

from configs import kafka_config
from confluent_kafka import Producer 
from datetime import datetime

# Функція генерації випадкових даних сенсора 
def generate_data(sensor_id): 
    return { 
        "sensor_id": sensor_id, 
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
        "temperature": round(random.uniform(25, 45),1), 
        "humidity": round(random.uniform(15, 85),1) 
        }

# Ідентифікатор сенсора
sensor_id = random.randint(1, 100)

# Створення продюсера Kafka 
producer = Producer(kafka_config)

def delivery_report(err, msg): 
    if err is not None: 
        print(f"Delivery failed for message {msg.key()}: {err}") 
    else: 
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Імітація роботи сенсора
try:
    while True:
        data = generate_data(sensor_id)
        producer.produce('vvd_building_sensors', value=json.dumps(data).encode('utf-8'), callback=delivery_report)
        producer.poll(1)  # Очікування подій протягом 1 секунди
        print(f"Sent data: {data}")
        time.sleep(5)  # Відправлення даних кожні 5 секунд
except KeyboardInterrupt:
    print("Stopped by user")
finally:
    producer.flush()  # Очікування завершення відправлення всіх повідомлень
