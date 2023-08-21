import coda_queue as q


@q.coda_task("sum_two_numbers")
def sum_two_numbers(a, b):
    return a + b
