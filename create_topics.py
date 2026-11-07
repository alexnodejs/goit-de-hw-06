# Створення вхідного та вихідного топіків. Повторний запуск безпечний.
from kafka import KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError

from configs import ALERTS_TOPIC, MY_NAME, SENSORS_TOPIC, kafka_config

admin_client = KafkaAdminClient(**kafka_config)

for topic in (SENSORS_TOPIC, ALERTS_TOPIC):
    try:
        # replication_factor=1, бо брокер один
        admin_client.create_topics(
            {topic: {"num_partitions": 2, "replication_factor": 1}},
            wait_for_metadata=True,
        )
        print(f"Створено топік: {topic}")
    except TopicAlreadyExistsError:
        print(f"Топік вже існує: {topic}")

print(f'\nТопіки, що містять "{MY_NAME}_hw06":')
for topic in admin_client.list_topics():
    if f"{MY_NAME}_hw06" in topic:
        print(topic)

admin_client.close()
