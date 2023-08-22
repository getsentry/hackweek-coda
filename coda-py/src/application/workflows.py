from application.tasks import sum_two_numbers
from core.coda_workflow import coda_workflow


@coda_workflow(workflow_name="MyWorkflow")
class MyWorkflow:

    async def run(self, a, b):
        task_handle = self.context.spawn_task(
            sum_two_numbers,
            [a, b],
            {"a": a, "b": b}
        )
        await self.context.await_one(task_handle)
