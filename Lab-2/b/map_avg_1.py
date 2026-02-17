import sys

NUM_PARTITIONS = 256


def mapper():
    """Mapper фазы 1.

    Ожидаемый вход: <value>
    Выход: <partition_id>\t<value>

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
