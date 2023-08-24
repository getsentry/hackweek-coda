from application.tasks import sum_two_numbers
from coda.workflow import coda_workflow


@coda_workflow(workflow_name="MyWorkflow")
async def my_workflow(context, x):
    sum_task = await context.spawn_task(
        task_function=sum_two_numbers,
        a=x * 10,
        b=x + 30
    )

    result = await sum_task
    print(f"Workflow has completed with result {result}")
