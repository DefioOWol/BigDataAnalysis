import sys


def mapper():
    """Mapper фазы 2.

    Ожидаемый вход: <value>
    Выход: max\t<value>

    Добавляем ключ `max`, чтобы все значения попали в один reducer.

    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        print(f"max\t{line}")


if __name__ == "__main__":
    mapper()
