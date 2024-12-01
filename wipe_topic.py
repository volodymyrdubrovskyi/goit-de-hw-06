from confluent_kafka.admin import AdminClient, NewTopic

# Налаштування конфігурації адміністратора Kafka
conf = {
    'bootstrap.servers': '77.81.230.104:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_PLAINTEXT',
    'sasl.username': 'admin',
    'sasl.password': 'VawEzo1ikLtrA8Ug8THa'
}

# Створення адміністративного клієнта Kafka
admin_client = AdminClient(conf)

# Ім'я топіка для видалення
topic = "vvd_building_sensors_alerts"

# Видалення топіка
fs = admin_client.delete_topics([topic], operation_timeout=30)

# Очікування результату операції видалення
for topic, f in fs.items():
    try:
        f.result()
        print(f"Топік {topic} успішно видалено")
    except Exception as e:
        print(f"Не вдалося видалити топік {topic}: {e}")
