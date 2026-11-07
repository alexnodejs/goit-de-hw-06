# Спільні налаштування всіх скриптів.
kafka_config = {"bootstrap_servers": "localhost:9094"}  # брокер з docker-compose.yml

MY_NAME = "vasileyko"  # ідентифікатор в іменах топіків, щоб вони не дублювалися

# Окремі від ДЗ5 топіки: там поле називається sensor_id, тут id за умовою.
SENSORS_TOPIC = f"{MY_NAME}_hw06_building_sensors"
ALERTS_TOPIC = f"{MY_NAME}_hw06_alerts"
