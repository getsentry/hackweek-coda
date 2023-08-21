import coda_queue as q


@q.coda_workflow(workflow_name="MyWorkflow")
class MyWorkflow:

    def run(self):
        pass