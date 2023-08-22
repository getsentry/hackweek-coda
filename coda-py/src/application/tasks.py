from core.coda_task import coda_task


@coda_task("sum_two_numbers")
async def sum_two_numbers(a, b):
    return a + b
