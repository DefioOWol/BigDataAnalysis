import sys


def mapper():
    """Mapper фазы 2.

    Ожидаемый вход: <partial_sum>\t<partial_count>
    Выход: avg\t<partial_sum>\t<partial_count>

    Добавляем ключ `avg`, чтобы все значения попали в один reducer.

    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        print(f"avg\t{line}")


if __name__ == "__main__":
    mapper()
