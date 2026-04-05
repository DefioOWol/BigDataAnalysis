"""Импорт из CSV + JSON, очистка, витрины, экспорт в Parquet/CSV/JSON."""

from pathlib import Path

from pyspark.sql import Column, SparkSession, Window
from pyspark.sql import functions as F

from domain import (
    CATEGORY_KEYS,
    CLEAN_DIR,
    EXPORT_DIR,
    MART_DIR,
    PRICES_CSV,
    PRODUCTS_JSON,
    REVIEWS_JSON,
)


def get_category_canonical_column() -> Column:
    """Получить каноническую категорию из raw категории."""
    c = F.lower(F.trim(F.col("category_raw")))
    e = F.lit(None).cast("string")
    for from_val, to_val in reversed(CATEGORY_KEYS):
        e = F.when(c == F.lit(from_val), F.lit(to_val)).otherwise(e)
    return e


def main():
    # Создаем директории для очищенных, витринных и экспортированных данных
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)
    MART_DIR.mkdir(parents=True, exist_ok=True)
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # Создаем Spark сессию
    spark = SparkSession.builder.config(
        "spark.sql.shuffle.partitions", "8"
    ).getOrCreate()

    # Читаем структурированный CSV с ценами
    df = spark.read.option("header", True).option("encoding", "UTF-8").csv(
        str(PRICES_CSV)
    )

    # Читаем структурированный справочник товаров
    products = spark.read.json(str(PRODUCTS_JSON))

    # Читаем JSON с неструктурированными отзывами
    reviews = spark.read.json(str(REVIEWS_JSON))

    # Очищаем данные
    cleaned = (
        df.select(
            F.trim(F.col("date")).alias("date_raw"),
            F.lower(F.trim(F.col("store"))).alias("store"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.trim(F.col("product")).alias("product"),
            F.trim(F.col("category")).alias("category_raw"),
            F.trim(F.col("price")).alias("price_raw"),
            F.lower(F.trim(F.col("availability"))).alias("availability_raw"),
            F.lower(F.trim(F.col("promo"))).alias("promo_raw"),
        )
        .withColumn("category", get_category_canonical_column())
        .withColumn(
            "price",
            F.when(F.col("price_raw") == "", None).otherwise(
                F.col("price_raw").cast("double")
            ),
        )
        .withColumn("event_date", F.to_date(F.col("date_raw"), "yyyy-MM-dd"))
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
        .filter(F.col("product_id") != "")
        .dropDuplicates(
            ["event_date", "store", "product_id", "category", "price"]
        )
        .select(
            "event_date",
            "store",
            "product_id",
            "product",
            "category",
            "price",
            "availability",
            "promo",
        )
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
    w = Window.partitionBy("store", "product_id").orderBy("event_date")
    with_lag = cleaned.withColumn(
        "prev_price", F.lag("price").over(w)
    ).withColumn(
        "prev_date", F.lag("event_date").over(w)
    )
    changes = (
        with_lag.filter(F.col("prev_price").isNotNull())
        .withColumn("delta", F.col("price") - F.col("prev_price"))
        .withColumn(
            "delta_percent",
            (F.col("price") - F.col("prev_price")) / F.col("prev_price") * 100,
        )
        .select(
            "event_date",
            "prev_date",
            "store",
            "product_id",
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

    # Собираем признаки отзывов
    text_col = F.coalesce(F.col("text"), F.lit(""))
    words = F.when(
        F.length(F.trim(text_col)) == 0,
        F.lit(0),
    ).otherwise(F.size(F.split(F.trim(text_col), "\\s+")))
    reviews_feat = reviews.select(
        "product_id",
        "store",
        "rating",
        F.length(text_col).alias("text_len"),
        words.alias("word_cnt"),
    )

    # Рассчитываем агрегаты по товару
    reviews_by_product = reviews_feat.groupBy("product_id").agg(
        F.count(F.lit(1)).alias("review_cnt"),
        F.avg("rating").alias("avg_rating"),
        F.avg("text_len").alias("avg_text_len"),
        F.avg("word_cnt").alias("avg_word_cnt"),
    )

    # Выбираем необходимые поля из справочника
    products_selected = products.select(
        F.col("product_id"),
        F.col("brand").alias("product_brand"),
        F.col("shelf_life_days").cast("int"),
    )

    # Объединяем данные очищенных цен и справочника
    cleaned_dim = cleaned.join(products_selected, on="product_id", how="left")
    enriched = cleaned_dim.join(reviews_by_product, on="product_id", how="left")

    # Собираем агрегаты по товару
    product_snapshot = enriched.groupBy(
        "product_id", "category", "product_brand"
    ).agg(
        F.avg("price").alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.countDistinct("store").alias("store_cnt"),
        F.max("review_cnt").alias("review_cnt"),
        F.max("avg_rating").alias("avg_rating"),
        F.max("avg_text_len").alias("avg_review_text_len"),
    )

    # Сохраняем витрину по товару
    product_snapshot.write.mode("overwrite").parquet(
        str(MART_DIR / "product_snapshot")
    )

    # Экспортируем витрины в JSON и CSV
    by_cat.coalesce(1).write.mode("overwrite").json(
        str(EXPORT_DIR / "daily_by_category")
    )
    product_snapshot.coalesce(1).write.mode("overwrite").csv(
        str(EXPORT_DIR / "product_snapshot"),
        header=True,
        encoding="UTF-8",
    )

    # Сохраняем список импортированных файлов
    manifest_lines = [
        f"prices_csv={PRICES_CSV.resolve()}",
        f"products_json={PRODUCTS_JSON.resolve()}",
        f"reviews_json={REVIEWS_JSON.resolve()}",
    ]
    Path(EXPORT_DIR / "import_manifest.txt").write_text(
        "\n".join(manifest_lines) + "\n", encoding="utf-8"
    )

    spark.stop()


if __name__ == "__main__":
    main()
