from coda.task import coda_task


@coda_task(task_name="sum_two_numbers")
async def sum_two_numbers(a, b):
    return a + b
