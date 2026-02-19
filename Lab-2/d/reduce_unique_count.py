import sys


def reducer():
    """Reducer для подсчета количества различных чисел.

    Ожидаемый вход: <value> / count
    Выход: count / <count>

    Если на входе передается число (фаза 1), то один раз для всех
    одинаковых чисел выводим `count`.
    Во второй фазе считаем входы и выводим только одно итоговое число.

    """
    current_key = None
    count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        count += 1
        if line != current_key:
            if line != "count":
                print("count")
            current_key = line

    if current_key == "count":
        print(count)


if __name__ == "__main__":
    reducer()
