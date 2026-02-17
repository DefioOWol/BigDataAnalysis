import sys


def mapper():
    """Mapper фазы 1.

    Ожидаемый вход: <value>
    Выход: <value>

    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            value = int(line)
        except ValueError:
            continue
        print(value)


if __name__ == "__main__":
    mapper()
