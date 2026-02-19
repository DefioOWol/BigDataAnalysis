import sys


def reducer():
    """Reducer для поиска максимального числа.

    Ожидаемый вход: <key>\t<value>
    Выход: <max>

    Ищет максимальное значение из входных данных.
    В первой фазе ищет локальный максимум,
    во второй - глобальный.

    """
    current_max = None

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        value = int(line.split("\t", maxsplit=1)[1])
        if current_max is None or value > current_max:
            current_max = value

    if current_max is not None:
        print(current_max)


if __name__ == "__main__":
    reducer()
