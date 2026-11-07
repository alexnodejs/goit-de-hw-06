# Перевірка вікон і фільтрації на пакетних даних, брокер не потрібен: python test_alert_stream.py
import json
from datetime import datetime

from alert_stream import (
    create_spark_session,
    find_alerts,
    load_conditions,
    to_kafka_messages,
    window_averages,
)

spark = create_spark_session()
spark.sparkContext.setLogLevel("ERROR")
conditions = load_conditions(spark)


def averages_of(t_avg, h_avg):
    window = (datetime(2026, 1, 1, 12, 0), datetime(2026, 1, 1, 12, 1))
    return spark.createDataFrame(
        [(window, float(t_avg), float(h_avg))],
        "window STRUCT<start: TIMESTAMP, end: TIMESTAMP>, t_avg DOUBLE, h_avg DOUBLE",
    )


def codes(t_avg, h_avg):
    return sorted(row.code for row in find_alerts(averages_of(t_avg, h_avg), conditions).collect())


# У межах норми алерту немає, -999 не спрацьовує як межа
assert codes(35, 50) == []

# Кожен алерт з файлу, межі включно
assert codes(35, 40) == ["101"]
assert codes(35, 40.01) == []
assert codes(35, 60) == ["102"]
assert codes(35, 59.99) == []
assert codes(30, 50) == ["103"]
assert codes(30.01, 50) == []
assert codes(40, 50) == ["104"]
assert codes(39.99, 50) == []

# Одне вікно може дати два алерти
assert codes(42, 70) == ["102", "104"]
assert codes(25, 30) == ["101", "103"]

# Умови беруться з файлу, а не з коду
custom = spark.createDataFrame(
    [(9, -999.0, 10.0, -999.0, -999.0, "999", "custom")], conditions.schema
)
assert [row.code for row in find_alerts(averages_of(35, 5), custom).collect()] == ["999"]
assert find_alerts(averages_of(35, 11), custom).collect() == []

# Вікно 1 хв із кроком 30 с: кожен показник потрапляє у два вікна
readings = spark.createDataFrame(
    [
        (datetime(2026, 1, 1, 12, 0, 10), 30.0, 40.0),
        (datetime(2026, 1, 1, 12, 0, 40), 40.0, 60.0),
    ],
    "timestamp TIMESTAMP, temperature DOUBLE, humidity DOUBLE",
)
windows = {
    (row.window.start.strftime("%H:%M:%S"), row.window.end.strftime("%H:%M:%S")): (row.t_avg, row.h_avg)
    for row in window_averages(readings).collect()
}
assert windows == {
    ("11:59:30", "12:00:30"): (30.0, 40.0),
    ("12:00:00", "12:01:00"): (35.0, 50.0),
    ("12:00:30", "12:01:30"): (40.0, 60.0),
}

# Повідомлення для Kafka містить усі поля алерту
value = to_kafka_messages(find_alerts(averages_of(41.126, 50), conditions)).first().value
message = json.loads(value)
assert set(message) == {"window", "t_avg", "h_avg", "code", "message", "timestamp"}
assert (message["t_avg"], message["code"], message["message"]) == (41.13, "104", "It's too hot")

spark.stop()
print("OK")
