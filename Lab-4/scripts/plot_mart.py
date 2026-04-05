"""Построение графиков по витринам."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from domain import MART_DIR, PLOTS_DIR, REVIEW_ROWS


def read_mart_parquet(folder: Path) -> pd.DataFrame:
    """Прочитать витрину из каталога с part-*.parquet."""
    return pd.read_parquet(folder, engine="pyarrow")


def main():
    # Создаем директорию для графиков
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # Читаем витрины
    by_cat = read_mart_parquet(MART_DIR / "daily_by_category")
    by_store = read_mart_parquet(MART_DIR / "daily_by_store")

    # Строим график средней цены по дням и категориям
    fig, ax = plt.subplots(figsize=(10, 5))
    for cat, g in by_cat.groupby("category"):
        g = g.sort_values("event_date")
        ax.plot(g["event_date"], g["avg_price"], label=cat, alpha=0.85)

    # Устанавливаем заголовок и оси
    ax.set_title("Средняя цена по дням и категориям")
    ax.set_xlabel("Дата")
    ax.set_ylabel("Средняя цена")
    ax.legend(fontsize=8)
    fig.autofmt_xdate()
    fig.tight_layout()

    # Сохраняем график
    fig.savefig(PLOTS_DIR / "daily_avg_by_category.jpg")
    plt.close(fig)

    # Строим график средней цены по магазинам
    fig, ax = plt.subplots(figsize=(8, 4))
    mean_by_store = by_store.groupby("store")["avg_price"].mean().sort_values()
    mean_by_store.plot(kind="barh", ax=ax, color="steelblue")

    # Устанавливаем заголовок и оси
    ax.set_title("Средняя цена по магазинам")
    ax.set_xlabel("Средняя цена")
    fig.tight_layout()

    # Сохраняем график
    fig.savefig(PLOTS_DIR / "avg_by_store.jpg")
    plt.close(fig)

    # Витрина product_snapshot
    snap = read_mart_parquet(MART_DIR / "product_snapshot")

    # Диаграмма рассеяния: средняя цена vs средний рейтинг
    # (размер точки ~ числу отзывов)
    fig, ax = plt.subplots(figsize=(10, 6))

    # Строим диаграмму рассеяния для каждой категории
    for cat, g in snap.groupby("category"):
        sizes = g["review_cnt"].astype(float) / REVIEW_ROWS * 400 + 10
        ax.scatter(
            g["avg_price"],
            g["avg_rating"],
            s=sizes,
            label=cat,
        )

    # Устанавливаем заголовок и оси
    ax.set_xlabel("Средняя цена")
    ax.set_ylabel("Средний рейтинг отзывов")
    ax.set_title("Товары: средняя цена и рейтинг (размер ~ число отзывов)")
    ax.legend(fontsize=8, title="Категория", loc="best")

    # Сохраняем график
    fig.savefig(PLOTS_DIR / "product_snapshot_price_rating.jpg")
    plt.close(fig)

    # Вычисляем относительный разброс цены
    spread = (
        (snap["max_price"] - snap["min_price"]) / snap["avg_price"] * 100.0
    )

    # Добавляем относительный разброс цены в витрину
    snap["spread_pct"] = spread
    snap = snap.sort_values("spread_pct")

    # Строим горизонтальную столбчатую диаграмму
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(snap["product_id"], snap["spread_pct"], color="steelblue")

    # Устанавливаем заголовок и оси
    ax.set_xlabel("Разброс цены, % от средней")
    ax.set_ylabel("Товар")
    ax.set_title("Изменчивость цен по товарам")

    # Сохраняем график
    fig.savefig(PLOTS_DIR / "product_snapshot_price_spread.jpg")
    plt.close(fig)


if __name__ == "__main__":
    main()
