# Етап 1: імітація одного датчика. Кілька датчиків = кілька запусків скрипта.
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from configs import SENSORS_TOPIC, kafka_config

SEND_INTERVAL_SECONDS = 2
TEMPERATURE_RANGE = (25, 45)
HUMIDITY_RANGE = (15, 85)
TEMPERATURE_STEP = 1.5
HUMIDITY_STEP = 4


def drift(value, step, value_range):
    """Наступний показник: поруч із попереднім і в межах діапазону.

    Незалежні випадкові значення за хвилинне вікно усереднюються приблизно до 35°C і 50%,
    тобто в норму, і алертів майже не буде. Плавна зміна дає вікна і в нормі, і поза нею.
    """
    low, high = value_range
    return min(high, max(low, value + random.uniform(-step, step)))


sensor_id = random.randint(1000, 9999)  # не змінюється протягом одного запуску
temperature = random.uniform(*TEMPERATURE_RANGE)
humidity = random.uniform(*HUMIDITY_RANGE)

producer = KafkaProducer(**kafka_config)

print(f"Датчик {sensor_id} запущено, топік {SENSORS_TOPIC}. Ctrl+C — зупинити.")
try:
    while True:
        temperature = drift(temperature, TEMPERATURE_STEP, TEMPERATURE_RANGE)
        humidity = drift(humidity, HUMIDITY_STEP, HUMIDITY_RANGE)
        reading = {
            "id": sensor_id,
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        # .get() чекає підтвердження брокера: помилка відправки не загубиться мовчки.
        meta = producer.send(
            SENSORS_TOPIC,
            key=str(sensor_id).encode(),
            value=json.dumps(reading).encode(),
        ).get(timeout=10)
        print(
            f"Надіслано в {meta.topic} [партиція {meta.partition}, offset {meta.offset}]: "
            f"id={sensor_id}, {reading['timestamp']}, "
            f"t={reading['temperature']}°C, h={reading['humidity']}%"
        )
        time.sleep(SEND_INTERVAL_SECONDS)
except KeyboardInterrupt:
    print(f"Датчик {sensor_id} зупинено.")
finally:
    producer.close()
