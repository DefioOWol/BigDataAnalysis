"""Генерация случайных данных цен товаров в магазинах."""

import csv
import random
from datetime import date, timedelta
from pathlib import Path


ROWS = 1_000_000

RANDOM_SEED = 42

OUT_PATH = Path(__file__).resolve().parent.parent / "data" / "raw" / "prices.csv"

AVAILABILITY_THRESHOLD = 0.07
PROMO_THRESHOLD = 0.82
DUPLICATE_THRESHOLD = 0.04

# Канонические категории и их варианты написания в данных
CATEGORY_CANON = [
    "молочные",
    "бакалея",
    "напитки",
    "овощи_фрукты",
    "бытовая_химия",
]
CATEGORY_VARIANTS = {
    "молочные": ["молочные", "Молочные", "МОЛОЧНЫЕ", "молочное"],
    "бакалея": ["бакалея", "Бакалея", "БАКАЛЕЯ"],
    "напитки": ["напитки", "Напитки", "НАПИТКИ", "напиток"],
    "овощи_фрукты": ["овощи_фрукты", "Овощи_фрукты", "ОВОЩИ", "фрукты"],
    "бытовая_химия": ["бытовая_химия", "Бытовая химия", "химия"],
}

PRODUCTS = {
    "молочные": ["молоко 2.5%", "кефир", "творог 5%", "сметана 20%", "йогурт клубника"],
    "бакалея": ["гречка 900г", "рис круглозерный", "макароны;рожки", "сахар 1кг", "мука в/с"],
    "напитки": ["вода 1.5л", "сок яблочный 1л", "лимонад 2л", "чай черный 100п"],
    "овощи_фрукты": ["картофель", "морковь", "яблоки антоновка", "бананы", "лук репчатый"],
    "бытовая_химия": ["порошок 3кг", "гель для посуды", "мыло жидкое", "отбеливатель"],
}

STORES = ["лента", "пятерочка", "магнит", "ашан", "перекресток"]

# Базовая цена в зависимости от магазина
PRICE_BASE_VALUES = dict(zip(STORES, [95, 92, 90, 88, 100]))


def pick_category(random_: random.Random) -> tuple[str, str]:
    """Выбрать случайные каноническую категорию и ее вариант написания."""
    canon = random_.choice(CATEGORY_CANON)
    label = random_.choice(CATEGORY_VARIANTS[canon])
    return canon, label


def random_date_between(random_: random.Random, start: date, end: date) -> date:
    """Выбрать случайный день между start и end."""
    return start + timedelta(days=random_.randint(0, max(0, (end - start).days)))


def main():
    # Инициализируем генератор случайных чисел
    random_ = random.Random(RANDOM_SEED)

    # Создаем директорию для выходного файла, если она не существует
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Определяем диапазон дат
    end = date.today()
    start = end - timedelta(days=365)

    # Определяем поля для выходного файла
    fields = ["date", "store", "product", "category", "price", "availability", "promo"]

    with OUT_PATH.open("w", newline="", encoding="utf-8") as f:
        # Создаем writer для записи в CSV с указанными полями
        writer = csv.DictWriter(f, fields)
        writer.writeheader()

        for _ in range(ROWS):
            # Выбираем случайные данные
            canon, label = pick_category(random_)
            product = random_.choice(PRODUCTS[canon])
            store = random_.choice(STORES)
            date_ = random_date_between(random_, start, end)

            # Генерируем случайную цену для продукта в магазине
            price_val = max(
                1.0,
                random_.gauss(PRICE_BASE_VALUES[store] + hash(product) % 40, 12),
            )

            # Генерируем случайную доступность и промо для продукта
            availability = "1" if random_.random() > AVAILABILITY_THRESHOLD else "0"
            promo = "yes" if random_.random() > PROMO_THRESHOLD else "no"

            date_str = date_.isoformat()
            price_str = f"{price_val:.2f}"

            # Генерируем случайные значения для некорректных данных
            if (roll := random_.random()) < 0.04:
                date_str = random_.choice(["2025-13-40", "н/д", "", "31.02.2025", "2024/03/15"])
            if roll < 0.05:
                price_str = ""
            if roll < 0.02:
                price_str = f"{-abs(price_val):.2f}"
            if roll < 0.03:
                availability = random_.choice(["", "да", "2", "maybe"])
            if roll < 0.02:
                promo = random_.choice(["", "YES", "Нет", "1"])

            # Создаем словарь с данными
            row = {
                "date": date_str,
                "store": store,
                "product": product,
                "category": label,
                "price": price_str,
                "availability": availability,
                "promo": promo,
            }
            writer.writerow(row)

            # С вероятностью DUPLICATE_THRESHOLD генерируем дубликат строки
            if random_.random() < DUPLICATE_THRESHOLD:
                writer.writerow(row)


if __name__ == "__main__":
    main()
