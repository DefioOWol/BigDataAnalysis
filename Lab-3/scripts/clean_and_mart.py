"""Очистка CSV и расчет витрин."""

from pathlib import Path

from pyspark.sql import Column, SparkSession, Window
from pyspark.sql import functions as F


# Определяем пути к данным
BASE = Path(__file__).resolve().parent.parent
RAW_CSV = BASE / "data" / "raw" / "prices.csv"
CLEAN_DIR = BASE / "data" / "clean"
MART_DIR = BASE / "data" / "mart"

# Указываем варианты написания категорий и их канонические значения
CATEGORY_KEYS = [
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


def get_category_canonical_column() -> Column:
    """Получить каноническую категорию из raw категории."""
    c = F.lower(F.trim(F.col("category_raw")))
    e = F.lit(None).cast("string")
    for from_val, to_val in reversed(CATEGORY_KEYS):
        e = F.when(c == F.lit(from_val), F.lit(to_val)).otherwise(e)
    return e


def main():
    # Создаем директории для очищенных и витринных данных
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    MART_DIR.mkdir(parents=True, exist_ok=True)

    # Создаем Spark сессию
    spark = SparkSession.builder.config("spark.sql.shuffle.partitions", "8").getOrCreate()

    # Читаем исходные данные
    df = spark.read.option("header", True).option("encoding", "UTF-8").csv(str(RAW_CSV))

    # Очищаем данные
    cleaned = (
        df.select(
            F.trim(F.col("date")).alias("date_raw"),
            F.lower(F.trim(F.col("store"))).alias("store"),
            F.trim(F.col("product")).alias("product"),
            F.trim(F.col("category")).alias("category_raw"),
            F.trim(F.col("price")).alias("price_raw"),
            F.lower(F.trim(F.col("availability"))).alias("availability_raw"),
            F.lower(F.trim(F.col("promo"))).alias("promo_raw"),
        )
        .withColumn("category", get_category_canonical_column())
        .withColumn(
            "price",
            F.when(F.col("price_raw") == "", None).otherwise(F.col("price_raw").cast("double")),
        )
        .withColumn(
            "event_date",
            F.to_date(F.col("date_raw"), "yyyy-MM-dd"),
        )
        .withColumn(
            "availability",
            F.when(F.col("availability_raw").isin("1", "yes", "да"), 1)
            .when(F.col("availability_raw").isin("0", "no", "нет"), 0)
            .otherwise(None),
        )
        .withColumn(
            "promo",
            F.when(F.col("promo_raw").isin("yes", "1", "да"), True)
            .when(F.col("promo_raw").isin("no", "0", "нет"), False)
            .otherwise(None),
        )
        .filter(F.col("event_date").isNotNull())
        .filter(F.col("price").isNotNull())
        .filter(F.col("price") > 0)
        .filter(F.col("category").isNotNull())
        .filter(F.col("store") != "")
        .filter(F.col("product") != "")
        .dropDuplicates(["event_date", "store", "product", "category", "price"])
        .select("event_date", "store", "product", "category", "price", "availability", "promo")
    )

    # Сохраняем очищенные данные
    cleaned.write.mode("overwrite").parquet(str(CLEAN_DIR / "prices_clean"))

    # Рассчитываем витрины
    # Витрина по категориям
    by_cat = cleaned.groupBy("event_date", "category").agg(
        F.avg("price").alias("avg_price"),
        F.count(F.lit(1)).alias("rows_cnt"),
    )
    # Витрина по магазинам
    by_store = cleaned.groupBy("event_date", "store").agg(
        F.avg("price").alias("avg_price"),
        F.count(F.lit(1)).alias("rows_cnt"),
    )

    # Витрина по изменениям цены
    w = Window.partitionBy("store", "product").orderBy("event_date")
    with_lag = cleaned.withColumn("prev_price", F.lag("price").over(w)).withColumn(
        "prev_date", F.lag("event_date").over(w)
    )

    # Витрина по изменениям цены
    changes = (
        with_lag.filter(F.col("prev_price").isNotNull())
        .withColumn("delta", F.col("price") - F.col("prev_price"))
        .withColumn("delta_percent", (F.col("price") - F.col("prev_price")) / F.col("prev_price") * 100)
        .select(
            "event_date",
            "prev_date",
            "store",
            "product",
            "category",
            "prev_price",
            "price",
            "delta",
            "delta_percent",
        )
    )

    # Сохраняем витрины
    by_cat.write.mode("overwrite").parquet(str(MART_DIR / "daily_by_category"))
    by_store.write.mode("overwrite").parquet(str(MART_DIR / "daily_by_store"))
    changes.write.mode("overwrite").parquet(str(MART_DIR / "price_changes_day"))

    spark.stop()


if __name__ == "__main__":
    main()
