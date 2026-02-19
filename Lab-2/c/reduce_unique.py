import sys


def reducer():
    """Reducer для получения множества уникальных чисел.

    Ожидаемый вход: <key>\t<value>
    Выход: <unique_value>

    В фазе 1 входное значение является самим числом, поэтому
    данное число выводится только один раз, без дубликатов.
    В фазе 2 значением являются уникальные числа, поэтому
    они выводятся все.

    """
    current_key = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        value = line.split("\t", maxsplit=1)[1]
        if value != current_key:
            print(value)
            current_key = value


if __name__ == "__main__":
    reducer()
