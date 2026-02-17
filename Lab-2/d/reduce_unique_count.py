import sys


def reducer():
    """Reducer для подсчета количества различных чисел.

    Ожидаемый вход: <value> / count
    Выход: count / <count>

    """
    current_key = None
    count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        count += 1
        if line != current_key:
            if line != "count":
                print("count")
            current_key = line

    if current_key == "count":
        print(count)


if __name__ == "__main__":
    reducer()
