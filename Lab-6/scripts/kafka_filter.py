"""Фильтрация: prices.transformed -> prices.filtered."""

from domain import TOPIC_PRICES_FILTERED, TOPIC_PRICES_TRANSFORMED
from kafka_common import is_control_end, json_consumer, json_producer


def keep_row(r: dict) -> bool:
    """Проверить, является ли строка корректной."""
    if r.get("event_date") is None:
        return False
    price = r.get("price")
    if price is None or price <= 0:
        return False
    if r.get("category") is None:
        return False
    if not r.get("store"):
        return False
    if not r.get("product"):
        return False
    if not r.get("product_id"):
        return False
    return True


def main():
    # Создаем consumer для топика prices.transformed
    consumer = json_consumer(TOPIC_PRICES_TRANSFORMED, group_id="filter")
    # Создаем producer для топика prices.filtered
    producer = json_producer()

    # Счетчик строк
    n = 0

    try:
        for msg in consumer:
            payload = msg.value
            # Если это служебное сообщение конца батча, отправляем его в топик prices.filtered
            if is_control_end(payload):
                producer.send(TOPIC_PRICES_FILTERED, value=payload)
                producer.flush()
                continue

            # Если сообщение не является словарем, пропускаем
            if not isinstance(payload, dict):
                continue

            # Если строка некорректна, пропускаем
            if not keep_row(payload):
                continue

            # Отправляем строку в топик prices.filtered
            key = (payload.get("category") or "na").strip() or "na"
            producer.send(TOPIC_PRICES_FILTERED, key=key, value=payload)

            # Если счетчик строк кратен 5000, отправляем батч
            n += 1
            if n % 5000 == 0:
                producer.flush()

    finally:
        # Закрываем producer и consumer
        producer.close()
        consumer.close()


if __name__ == "__main__":
    main()
