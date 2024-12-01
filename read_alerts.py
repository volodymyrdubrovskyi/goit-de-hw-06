from confluent_kafka import Consumer, KafkaException
from configs import kafka_config
from copy import deepcopy

# Конфігурація Kafka Consumer
consumer_kafka_config = deepcopy(kafka_config)
consumer_kafka_config["group.id"] = "sensor-group"
consumer_kafka_config["auto.offset.reset"] = "earliest"

# Створення Kafka Consumer
consumer = Consumer(consumer_kafka_config)

# Підписка на топік
topic = 'vvd_building_sensors_alerts'
consumer.subscribe([topic])

# Функція для обробки повідомлень
def consume_messages():
    i = 1
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                print("no meggages...")
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Це кінцева частина розділу, означає що повідомлень більше немає
                    continue
                else:
                    raise KafkaException(msg.error())
            print(f"{i}: Received message: {msg.value().decode('utf-8')}")
            i = i + 1
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()

# Споживання повідомлень з топіку та виведення їх на консоль
consume_messages()