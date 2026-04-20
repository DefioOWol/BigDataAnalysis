"""Публикация строк prices.csv в топик prices.raw."""

import csv

from domain import CONTROL_END, PRICES_CSV, TOPIC_PRICES_RAW
from kafka_common import json_producer


def main():
    # Создаем producer для JSON-сообщений
    producer = json_producer()

    try:
        # Отправляем строки CSV в топик prices.raw
        with PRICES_CSV.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row.get("category") or "na").strip() or "na"
                producer.send(TOPIC_PRICES_RAW, key=key, value=row)

        # Отправляем служебное сообщение конца батча
        producer.send(TOPIC_PRICES_RAW, value=CONTROL_END)
        producer.flush()

    finally:
        # Закрываем producer
        producer.close()


if __name__ == "__main__":
    main()
