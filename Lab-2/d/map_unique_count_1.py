import sys


def mapper():
    """Mapper фазы 1.

    Ожидаемый вход: <value>
    Выход: <value>

    Получает строки (одно число на строку) и валидирует его,
    пытаясь привести к типу `int`. Если проверки проходят, то
    отдает его дальше.
    На выходе отдаем только число, которое будет являться и ключом.

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
