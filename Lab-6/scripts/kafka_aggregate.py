"""Агрегирование по категориям: prices.filtered -> JSON на диск."""

import json
from collections import defaultdict

from domain import KAFKA_OUT_DIR, TOPIC_PRICES_FILTERED
from kafka_common import is_control_end, json_consumer


def main():
    # Создаем директорию для агрегированных данных
    KAFKA_OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = KAFKA_OUT_DIR / "aggregates_by_category.json"

    # Создаем consumer для топика prices.filtered
    consumer = json_consumer(TOPIC_PRICES_FILTERED, group_id="aggregate")

    # Словари для сумм цен по категориям и количества строк по категориям
    sums = defaultdict(float)
    counts = defaultdict(int)

    try:
        # Обрабатываем сообщения из топика prices.filtered
        for msg in consumer:
            payload = msg.value

            # Если это служебное сообщение конца батча, записываем результаты в JSON
            if is_control_end(payload):
                rows = []
                for cat in sorted(counts.keys()):
                    c = counts[cat]
                    s = sums[cat]
                    rows.append(
                        {
                            "category": cat,
                            "rows": c,
                            "sum_price": round(s, 4),
                            "avg_price": round(s / c, 4) if c else 0,
                        }
                    )

                # Создаем документ JSON с результатами агрегации
                doc = {"by_category": rows, "total_filtered_rows": sum(counts.values())}
                # Записываем документ в файл
                out_path.write_text(
                    json.dumps(doc, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )

                # Очищаем словари сумм и количеств
                sums.clear()
                counts.clear()
                continue

            # Если сообщение не является словарем, пропускаем
            if not isinstance(payload, dict):
                continue

            # Получаем категорию и цену из сообщения
            cat = payload.get("category")
            price = payload.get("price")
            # Если категории или цены нет, пропускаем
            if not cat or not price:
                continue

            # Суммируем цену и увеличиваем количество строк по категории
            sums[cat] += float(price)
            counts[cat] += 1

    finally:
        # Закрываем consumer
        consumer.close()


if __name__ == "__main__":
    main()
