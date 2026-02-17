import sys


def mapper():
    """Mapper фазы 1.

    Ожидаемый вход: <value>
    Выход: <value>\t<value>

    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            value = int(line)
        except ValueError:
            continue
        print(f"{value}\t{value}")


if __name__ == "__main__":
    mapper()
