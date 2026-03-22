"""Построение графиков по витринам."""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


# Определяем пути к данным
BASE = Path(__file__).resolve().parent.parent
MART_DIR = BASE / "data" / "mart"
PLOTS_DIR = BASE / "data" / "plots"


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


if __name__ == "__main__":
    main()
