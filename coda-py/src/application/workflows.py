from application.tasks import sum_two_numbers
from core.coda_workflow import coda_workflow


@coda_workflow(workflow_name="MyWorkflow")
class MyWorkflow:

    async def run(self, context, a, b):
        task_handle = context.spawn_task(
            sum_two_numbers,
            [a, b],
            {"a": a, "b": b}
        )
        result = await context.await_one(task_handle)

        return result
