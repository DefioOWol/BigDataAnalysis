"""Общие пути и справочники для генерации данных и Spark-пайплайна."""

from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

# Пути к данным
RAW_DIR = BASE / "data" / "raw"
CLEAN_DIR = BASE / "data" / "clean"
MART_DIR = BASE / "data" / "mart"
EXPORT_DIR = BASE / "data" / "export"
PLOTS_DIR = BASE / "data" / "plots"

# Пути к файлам сырых данных
PRICES_CSV = RAW_DIR / "prices.csv"
PRODUCTS_JSON = RAW_DIR / "products.json"
REVIEWS_JSON = RAW_DIR / "reviews.json"

# Размеры данных и сид рандома
ROWS = 1_000_000
REVIEW_ROWS = 120_000
RANDOM_SEED = 42

# Пороговые значения
RANDOM_REWIEW_SYMBOL_THRESHOLD = 0.15
AVAILABILITY_THRESHOLD = 0.07
PROMO_THRESHOLD = 0.82
DUPLICATE_THRESHOLD = 0.04

# Магазины
STORES = ["лента", "пятерочка", "магнит", "ашан", "перекресток"]

# Базовые цены для магазинов
PRICE_BASE_VALUES = dict(zip(STORES, [95, 92, 90, 88, 100], strict=True))

# Канонические категории и варианты написания в CSV
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

# Справочник товаров: id, каноническая категория, отображаемое имя, бренд, срок годности (в днях)
PRODUCT_CATALOG: list[dict[str, str | int]] = [
    {"product_id": "prd_m_1", "category": "молочные", "name": "молоко 2.5%", "brand": "домик", "shelf_life_days": 5},
    {"product_id": "prd_m_2", "category": "молочные", "name": "кефир", "brand": "простоквашино", "shelf_life_days": 14},
    {"product_id": "prd_m_3", "category": "молочные", "name": "творог 5%", "brand": "домик", "shelf_life_days": 12},
    {"product_id": "prd_m_4", "category": "молочные", "name": "сметана 20%", "brand": "село", "shelf_life_days": 20},
    {"product_id": "prd_m_5", "category": "молочные", "name": "йогурт клубника", "brand": "активиа", "shelf_life_days": 30},
    {"product_id": "prd_b_1", "category": "бакалея", "name": "гречка 900г", "brand": "уладар", "shelf_life_days": 365},
    {"product_id": "prd_b_2", "category": "бакалея", "name": "рис круглозерный", "brand": "мистраль", "shelf_life_days": 730},
    {"product_id": "prd_b_3", "category": "бакалея", "name": "макароны;рожки", "brand": "барилла", "shelf_life_days": 540},
    {"product_id": "prd_b_4", "category": "бакалея", "name": "сахар 1кг", "brand": "красный", "shelf_life_days": 1000},
    {"product_id": "prd_b_5", "category": "бакалея", "name": "мука в/с", "brand": "богатырь", "shelf_life_days": 180},
    {"product_id": "prd_n_1", "category": "напитки", "name": "вода 1.5л", "brand": "аква", "shelf_life_days": 730},
    {"product_id": "prd_n_2", "category": "напитки", "name": "сок яблочный 1л", "brand": "добрый", "shelf_life_days": 120},
    {"product_id": "prd_n_3", "category": "напитки", "name": "лимонад 2л", "brand": "черноголовка", "shelf_life_days": 180},
    {"product_id": "prd_n_4", "category": "напитки", "name": "чай черный 100п", "brand": "липтон", "shelf_life_days": 400},
    {"product_id": "prd_o_1", "category": "овощи_фрукты", "name": "картофель", "brand": "ферма", "shelf_life_days": 45},
    {"product_id": "prd_o_2", "category": "овощи_фрукты", "name": "морковь", "brand": "ферма", "shelf_life_days": 30},
    {"product_id": "prd_o_3", "category": "овощи_фрукты", "name": "яблоки антоновка", "brand": "сад", "shelf_life_days": 60},
    {"product_id": "prd_o_4", "category": "овощи_фрукты", "name": "бананы", "brand": "эквадор", "shelf_life_days": 10},
    {"product_id": "prd_o_5", "category": "овощи_фрукты", "name": "лук репчатый", "brand": "ферма", "shelf_life_days": 90},
    {"product_id": "prd_h_1", "category": "бытовая_химия", "name": "порошок 3кг", "brand": "арiel", "shelf_life_days": 1095},
    {"product_id": "prd_h_2", "category": "бытовая_химия", "name": "гель для посуды", "brand": "фери", "shelf_life_days": 730},
    {"product_id": "prd_h_3", "category": "бытовая_химия", "name": "мыло жидкое", "brand": "див", "shelf_life_days": 365},
    {"product_id": "prd_h_4", "category": "бытовая_химия", "name": "отбеливатель", "brand": "белизна", "shelf_life_days": 730},
]

# Шаблоны отзывов (неструктурированный текст) - подстановка {product}, {store}
REVIEW_TEMPLATES = [
    "Купил {product} в {store}, качество нормальное, цена могла бы быть ниже.",
    "{store}: {product} свежий, упаковка целая, доволен покупкой.",
    "Не понравилось - {product} в {store} оказался с истекающим сроком.",
    "Отличный {product}, в {store} часто беру, рекомендую!",
    "Средне: {product} из {store}, ожидал лучше за эти деньги!",
    "Доставка ок, {product} в {store} как обычно.",
]

# Случайные символы для отзывов
REVIEW_SYMBOLS = ["!!!", "…", "??", " #скидка", ""]

# Нормализация категории в Spark
CATEGORY_KEYS: list[tuple[str, str]] = [
    ("молочные", "молочные"),
    ("молочное", "молочные"),
    ("бакалея", "бакалея"),
    ("напитки", "напитки"),
    ("напиток", "напитки"),
    ("овощи_фрукты", "овощи_фрукты"),
    ("овощи", "овощи_фрукты"),
    ("фрукты", "овощи_фрукты"),
    ("бытовая_химия", "бытовая_химия"),
    ("бытовая химия", "бытовая_химия"),
    ("химия", "бытовая_химия"),
]
