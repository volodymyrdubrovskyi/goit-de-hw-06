from confluent_kafka.admin import AdminClient, NewTopic
from configs import kafka_config

admin_client = AdminClient(kafka_config)

# Перевірка конекту
topics = admin_client.list_topics().topics
print(f"Connected to Kafka. \nAvailable topics #: {len(topics)}")

# Визначення нових топіків 
topic = NewTopic("vvd_building_sensors_alerts", num_partitions=1, replication_factor=1) 

# Створення нових топіків 
try: 
    admin_client.create_topics([topic], validate_only=False) 
except Exception as e: 
    print(f"An error occurred: {e}")
    
print("Перевірка створення топиків:")
metadata = admin_client.list_topics(timeout=10)
topics = metadata.topics

[print(topic) for topic in topics if "vvd_" in topic]