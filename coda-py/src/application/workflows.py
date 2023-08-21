import coda_queue as q
from coda_py.tasks import sum_two_numbers


@q.coda_workflow(workflow_name="MyWorkflow")
class MyWorkflow:

    def run(self, a, b):
        self.context.execute_task(
            sum_two_numbers,
            [a, b],
            {"a": a, "b": b}
        )
