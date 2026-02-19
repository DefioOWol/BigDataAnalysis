import sys


def reducer():
    """Reducer фазы 2.

    Ожидаемый вход: avg\t<partial_sum>\t<partial_count>
    Выход: <avg>

    Суммируем все частичные суммы и количества элементов.
    Выводим итоговое среднее арифметическое.

    """
    total_sum = total_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t", maxsplit=2)
        partial_sum, partial_count = map(int, parts[1:])

        total_sum += partial_sum
        total_count += partial_count

    if total_count > 0:
        print(total_sum / total_count)


if __name__ == "__main__":
    reducer()
