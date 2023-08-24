from application.tasks import sum_two_numbers, divide_by
from coda.workflow import coda_workflow


@coda_workflow(workflow_name="MyWorkflow")
async def my_workflow(context, x):
    a = x * 10
    b = x + 30

    task_handle = await context.spawn_task(
        sum_two_numbers,
        [a, b],
        {"a": a, "b": b}
    )
    sum = await task_handle

    by = 5
    task_2 = await context.spawn_task(
        divide_by,
        [sum, by],
        {"x": sum, "y": by}
    )
    division = await task_2

    print(f"Workflow has completed with result {division}")
