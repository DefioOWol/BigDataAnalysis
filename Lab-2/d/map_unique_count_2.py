import sys


def mapper():
    """Mapper фазы 2.

    Ожидаемый вход: count
    Выход: count

    В данной фазе нужно только считать данные и вернуть для каждого
    выхода с reducer первой фазы отображение входа для подсчета.

    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        print(line)


if __name__ == "__main__":
    mapper()
