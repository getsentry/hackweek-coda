from application.tasks import sum_two_numbers
from coda.workflow import coda_workflow


@coda_workflow(workflow_name="MyWorkflow")
async def my_workflow(context, a, b):
    task_handle = context.spawn_task(
        sum_two_numbers,
        [a, b],
        {"a": a, "b": b}
    )
    result = await task_handle
    return result
