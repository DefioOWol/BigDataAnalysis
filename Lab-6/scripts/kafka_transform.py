"""Трансформация: prices.raw -> prices.transformed."""

from datetime import datetime

from domain import CATEGORY_KEYS, TOPIC_PRICES_RAW, TOPIC_PRICES_TRANSFORMED
from kafka_common import is_control_end, json_consumer, json_producer


def parse_event_date(date_raw: str) -> str | None:
    """Преобразовать строку даты в формате YYYY-MM-DD в ISO-формат."""
    try:
        return datetime.strptime(date_raw.strip(), "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def parse_price(price_raw: str) -> float | None:
    """Преобразовать строку цены в число."""
    try:
        return float(price_raw.strip().replace(",", "."))
    except ValueError:
        return None


def parse_availability(raw: str | None) -> int | None:
    """Преобразовать строку доступности в число."""
    if raw is None:
        return None
    v = raw.strip().lower()
    if v in ("1", "yes", "да"):
        return 1
    if v in ("0", "no", "нет"):
        return 0
    return None


def parse_promo(raw: str | None) -> bool | None:
    """Преобразовать строку промо в булево значение."""
    if raw is None:
        return None
    v = raw.strip().lower()
    if v in ("yes", "1", "да"):
        return True
    if v in ("no", "0", "нет"):
        return False
    return None


def transform_row(raw: dict) -> dict:
    """Преобразовать строку в словарь."""
    date_raw = (raw.get("date") or "").strip()
    category_raw = (raw.get("category") or "").strip()
    key_cat = category_raw.lower()
    category = dict(CATEGORY_KEYS).get(key_cat)

    return {
        "date_raw": date_raw,
        "event_date": parse_event_date(date_raw),
        "store": (raw.get("store") or "").strip().lower(),
        "product_id": (raw.get("product_id") or "").strip(),
        "product": (raw.get("product") or "").strip(),
        "category_raw": category_raw,
        "category": category,
        "price_raw": (raw.get("price") or "").strip(),
        "price": parse_price(raw.get("price") or ""),
        "availability": parse_availability(raw.get("availability")),
        "promo": parse_promo(raw.get("promo")),
    }


def main():
    # Создаем consumer для топика prices.raw
    consumer = json_consumer(TOPIC_PRICES_RAW, group_id="transform")
    # Создаем producer для топика prices.transformed
    producer = json_producer()

    # Счетчик строк
    n = 0

    try:
        # Обрабатываем сообщения из топика prices.raw
        for msg in consumer:
            payload = msg.value

            # Если это служебное сообщение конца батча, отправляем его в топик prices.transformed
            if is_control_end(payload):
                producer.send(TOPIC_PRICES_TRANSFORMED, value=payload)
                producer.flush()
                continue

            # Если сообщение не является словарем, пропускаем
            if not isinstance(payload, dict):
                continue

            # Преобразовываем строку в словарь
            out = transform_row(payload)
            # Отправляем сообщение в топик prices.transformed
            key = out.get("category") or out.get("category_raw") or "na"
            producer.send(TOPIC_PRICES_TRANSFORMED, key=key, value=out)

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
