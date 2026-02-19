import sys


def mapper():
    """Mapper фазы 1.

    Ожидаемый вход: <value>
    Выход: <value>\t<value>

    Получает строки (одно число на строку) и валидирует его,
    пытаясь привести к типу `int`. Если проверки проходят, то
    отдает его дальше.
    На выходе дублируем число, для того, чтобы оно было
    ключом и значением. Это необходимо для того, чтобы можно было
    использовать единый reducer для обеих фаз.

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
