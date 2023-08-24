from coda.task import coda_task


@coda_task(task_name="sum_two_numbers")
async def sum_two_numbers(a, b):
    return a + b


@coda_task(task_name="divide_by")
async def divide_by(x, y):
    return x / y
