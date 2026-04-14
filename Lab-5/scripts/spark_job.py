"""Импорт из CSV + JSON, очистка, витрины, MLlib, экспорт в Parquet/CSV/JSON."""

import json
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator
from pyspark.ml.feature import (
    HashingTF,
    IDF,
    StandardScaler,
    StringIndexer,
    Tokenizer,
    VectorAssembler,
)
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Column, SparkSession, Window
from pyspark.sql import functions as F

from domain import (
    CATEGORY_KEYS,
    CLEAN_DIR,
    EXPORT_DIR,
    MART_DIR,
    ML_DIR,
    PRICES_CSV,
    PRODUCTS_JSON,
    RANDOM_SEED,
    REVIEWS_JSON,
    ML_SAMPLE_SIZE,
    ML_TRAIN_SIZE,
)


def get_category_canonical_column() -> Column:
    """Получить каноническую категорию из raw категории."""
    c = F.lower(F.trim(F.col("category_raw")))
    e = F.lit(None).cast("string")
    for from_val, to_val in reversed(CATEGORY_KEYS):
        e = F.when(c == F.lit(from_val), F.lit(to_val)).otherwise(e)
    return e


def run_mllib(spark: SparkSession):
    """Запустить MLlib.

    Объединение структурированного справочника и неструктурированного
    текста отзывов, кластеризация витрины по числовым признакам,
    сохранение метрик в JSON.

    """
    # Создаем директорию для MLlib и инициализируем словарь для метрик
    ML_DIR.mkdir(parents=True, exist_ok=True)
    metrics = {}

    # Готовим данные для линейной регрессии рейтинга:
    # TF-IDF по полю text + признаки из products.json
    # Читаем JSON с неструктурированными отзывами
    reviews = spark.read.json(str(REVIEWS_JSON))
    # Читаем структурированный справочник товаров
    products_ml = spark.read.json(str(PRODUCTS_JSON)).select(
        F.col("product_id"),
        F.col("category").alias("prod_category"),
        F.col("shelf_life_days").cast("double").alias("shelf_life_days"),
    )
    # Объединяем данные о отзывах и справочнике товаров
    joined = (
        reviews.join(products_ml, on="product_id", how="inner")
        .withColumn("rating", F.col("rating").cast("double"))
        .withColumn("text", F.coalesce(F.col("text"), F.lit("")))
        .filter(F.col("rating").isNotNull())
        .filter(F.col("prod_category").isNotNull())
    )
    # Разделяем данные на обучающую и тестовую выборки
    train, test = joined.randomSplit(
        [ML_TRAIN_SIZE, 1 - ML_TRAIN_SIZE], seed=RANDOM_SEED
    )

    # Создаем индекс категории товара
    cat_idx = StringIndexer(
        inputCol="prod_category", outputCol="catIdx", handleInvalid="keep"
    )
    # Токенизируем текст отзыва
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    # Создаем хэш-векторы для текста отзыва
    hashing_tf = HashingTF(
        inputCol="words", outputCol="raw_tf", numFeatures=1024
    )
    # Создаем IDF-векторы для текста отзыва
    idf = IDF(inputCol="raw_tf", outputCol="tfidf", minDocFreq=1)
    # Собираем признаки для линейной регрессии
    vec_lr = VectorAssembler(
        inputCols=["tfidf", "catIdx", "shelf_life_days"],
        outputCol="features",
    )

    # Создаем модель линейной регрессии
    lr = LinearRegression(
        featuresCol="features",
        labelCol="rating",
        maxIter=30,
        regParam=0.1,
    )
    # Создаем пайплайн для линейной регрессии
    pipe_lr = Pipeline(
        stages=[cat_idx, tokenizer, hashing_tf, idf, vec_lr, lr]
    )
    # Обучаем модель линейной регрессии
    model_lr = pipe_lr.fit(train)

    # Делаем предсказания на тестовой выборке
    pred_lr = model_lr.transform(test)
    # Считаем RMSE и R2
    rmse = RegressionEvaluator(
        labelCol="rating", predictionCol="prediction", metricName="rmse"
    ).evaluate(pred_lr)
    # Считаем R2
    r2 = RegressionEvaluator(
        labelCol="rating", predictionCol="prediction", metricName="r2"
    ).evaluate(pred_lr)

    # Сохраняем метрики линейной регрессии
    metrics["rating_regression"] = {
        "описание": "Предсказание rating по TF-IDF + категория + shelf_life_days",
        "rmse": rmse,
        "r2": r2,
        "train_rows": train.count(),
        "test_rows": test.count(),
    }
    # Сохраняем фрагмент предсказаний
    pred_lr.select("rating", "prediction", "text").limit(ML_SAMPLE_SIZE) \
        .coalesce(1).write.mode("overwrite") \
        .json(str(ML_DIR / "rating_predictions_sample"))

    # Готовим данные для кластеризации витрины product_snapshot
    # Читаем витрину по товару
    ps = spark.read.parquet(str(MART_DIR / "product_snapshot"))
    # Выбираем необходимые поля из витрины по товару
    feat_tbl = ps.select(
        "product_id",
        F.coalesce(F.col("avg_price"), F.lit(0.0)).alias("avg_price"),
        F.coalesce(F.col("avg_rating"), F.lit(0.0)).alias("avg_rating"),
        F.coalesce(F.col("review_cnt"), F.lit(0.0)).alias("review_cnt"),
    )
    # Собираем признаки для кластеризации
    vec_k = VectorAssembler(
        inputCols=["avg_price", "avg_rating", "review_cnt"],
        outputCol="features_raw",
    )
    # Нормализуем признаки
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withStd=True,
        withMean=True,
    )

    # Создаем модель кластеризации
    kmeans = KMeans(k=5, seed=RANDOM_SEED, featuresCol="features", predictionCol="cluster")
    # Создаем пайплайн для кластеризации
    pipe_kmeans = Pipeline(stages=[vec_k, scaler, kmeans])
    # Обучаем модель кластеризации
    model_kmeans = pipe_kmeans.fit(feat_tbl)

    # Делаем предсказания на тестовой выборке
    pred_kmeans = model_kmeans.transform(feat_tbl)
    # Считаем silhouette
    sil = ClusteringEvaluator(
        featuresCol="features",
        predictionCol="cluster",
        metricName="silhouette",
    ).evaluate(pred_kmeans)
    # Считаем WSSSE
    kmeans_stage = model_kmeans.stages[-1]
    wssse = float(kmeans_stage.summary.trainingCost)

    # Сохраняем метрики кластеризации
    metrics["product_clusters_5"] = {
        "описание": "Кластеры товаров по avg_price, avg_rating, review_cnt",
        "silhouette": sil,
        "wssse": wssse,
        "rows": feat_tbl.count(),
    }
    pred_kmeans.select(
        "product_id", "cluster", "avg_price", "avg_rating", "review_cnt"
    ).write.mode("overwrite").parquet(str(ML_DIR / "product_clusters"))

    # Сохраняем все метрики в JSON
    Path(ML_DIR / "metrics.json").write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


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

    # Запуск MLlib после построения витрин
    run_mllib(spark)

    spark.stop()


if __name__ == "__main__":
    main()
