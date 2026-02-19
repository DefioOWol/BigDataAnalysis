import sys

NUM_PARTITIONS = 256


def mapper():
    """Mapper фазы 1.

    Ожидаемый вход: <value>
    Выход: <partition_id>\t<value>

    Получает строки (одно число на строку) и валидирует его,
    пытаясь привести к типу `int`. Если проверки проходят, то
    отдает его дальше.
    Далее вычисляем hash функцию для числа, чтобы определить
    номер reducer, в который стоит отправить значение.
    Это делается для возможности распараллеливания.

    """
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            value = int(line)
        except ValueError:
            continue
        partition_id = abs(hash(value)) % NUM_PARTITIONS
        print(f"{partition_id}\t{value}")


if __name__ == "__main__":
    mapper()
