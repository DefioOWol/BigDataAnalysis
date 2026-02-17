import sys


def reducer():
    """Reducer фазы 1.

    Ожидаемый вход: <partition_id>\t<value>
    Выход: <partial_sum>\t<partial_count>

    """
    partial_sum = partial_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        value = int(line.split("\t", maxsplit=1)[1])
        partial_sum += value
        partial_count += 1

    if partial_count > 0:
        print(f"{partial_sum}\t{partial_count}")


if __name__ == "__main__":
    reducer()
