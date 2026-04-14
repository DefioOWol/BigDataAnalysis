"""Генерация набора данных: CSV цен, JSON справочника товаров, JSON отзывов."""

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

from domain import (
    AVAILABILITY_THRESHOLD,
    CATEGORY_VARIANTS,
    DUPLICATE_THRESHOLD,
    RANDOM_REWIEW_SYMBOL_THRESHOLD,
    PRICE_BASE_VALUES,
    PRODUCT_CATALOG,
    PRICES_CSV,
    PRODUCTS_JSON,
    PROMO_THRESHOLD,
    RANDOM_SEED,
    REVIEWS_JSON,
    REVIEW_SYMBOLS,
    REVIEW_TEMPLATES,
    REVIEW_ROWS,
    ROWS,
    STORES,
)


def pick_category_label(random_: random.Random, canon: str) -> str:
    """Выбрать случайный вариант написания каноничной категории."""
    return random_.choice(CATEGORY_VARIANTS[canon])


def random_date_between(random_: random.Random, start: date, end: date) -> date:
    """Выбрать случайный день между start и end."""
    return start + timedelta(
        days=random_.randint(0, max(0, (end - start).days))
    )


def write_products_catalog(path: Path):
    """Записать каталог товаров в JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for p in PRODUCT_CATALOG:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")


def write_reviews(path: Path, random_: random.Random):
    """Записать отзывы в JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)

    # Определяем диапазон дат
    end = date.today()
    start = end - timedelta(days=365)

    with path.open("w", encoding="utf-8") as f:
        for _ in range(REVIEW_ROWS):
            # Выбираем случайные товар, магазин и текст отзыва
            product = random_.choice(PRODUCT_CATALOG)
            store = random_.choice(STORES)
            text = random_.choice(REVIEW_TEMPLATES).format(
                product=product["name"],
                store=store,
            )

            # Добавляем случайный символ в текст отзыва
            if random_.random() < RANDOM_REWIEW_SYMBOL_THRESHOLD:
                text = text + " " + random_.choice(REVIEW_SYMBOLS)

            # Генерируем случайные рейтинг и дату создания отзыва
            rating = max(1, min(5, int(random_.gauss(3.5, 1.2))))
            created_at = random_date_between(random_, start, end).isoformat()

            # Создаем словарь с данными и записываем в JSON
            row = {
                "product_id": product["product_id"],
                "store": store,
                "rating": rating,
                "created_at": created_at,
                "text": text,
            }
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main():
    # Инициализируем генератор случайных чисел
    random_ = random.Random(RANDOM_SEED)

    # Записываем каталог товаров и отзывы
    write_products_catalog(PRODUCTS_JSON)
    write_reviews(REVIEWS_JSON, random_)

    # Создаем директорию для цен
    PRICES_CSV.parent.mkdir(parents=True, exist_ok=True)

    # Определяем диапазон дат
    end = date.today()
    start = end - timedelta(days=365)

    # Определяем поля для цен
    fields = [
        "date",
        "store",
        "product_id",
        "product",
        "category",
        "price",
        "availability",
        "promo",
    ]

    # Записываем цены в CSV
    with PRICES_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fields)
        writer.writeheader()

        for _ in range(ROWS):
            # Выбираем случайный товар
            product = random_.choice(PRODUCT_CATALOG)
            product_name = product["name"]

            # Выбираем случайный вариант написания каноничной категории
            canon = product["category"]
            label = pick_category_label(random_, canon)

            # Выбираем случайные магазин и дату
            store = random_.choice(STORES)
            date_ = random_date_between(random_, start, end)

            # Генерируем случайную цену для продукта в магазине
            price_val = max(
                1.0,
                random_.gauss(
                    PRICE_BASE_VALUES[store] + hash(product_name) % 40, 12
                ),
            )

            # Генерируем случайную доступность и промо для продукта
            availability = (
                "1" if random_.random() > AVAILABILITY_THRESHOLD else "0"
            )
            promo = "yes" if random_.random() > PROMO_THRESHOLD else "no"

            # Форматируем дату и цену
            date_str = date_.isoformat()
            price_str = f"{price_val:.2f}"

            # Генерируем случайные значения для некорректных данных
            roll = random_.random()
            if roll < 0.04:
                date_str = random_.choice(
                    ["2025-13-40", "н/д", "", "31.02.2025", "2024/03/15"]
                )
            if roll < 0.05:
                price_str = ""
            if roll < 0.02:
                price_str = f"{-abs(price_val):.2f}"
            if roll < 0.03:
                availability = random_.choice(["", "да", "2", "maybe"])
            if roll < 0.02:
                promo = random_.choice(["", "YES", "Нет", "1"])

            # Создаем словарь с данными и записываем в CSV
            row = {
                "date": date_str,
                "store": store,
                "product_id": product["product_id"],
                "product": product_name,
                "category": label,
                "price": price_str,
                "availability": availability,
                "promo": promo,
            }
            writer.writerow(row)

            # С определенной вероятностью генерируем дубликат строки
            if random_.random() < DUPLICATE_THRESHOLD:
                writer.writerow(row)


if __name__ == "__main__":
    main()
