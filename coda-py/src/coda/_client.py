from coda._interest import Listener
from coda._supervisor import Supervisor
from coda._workflow import WorkflowContext


class Client(Listener):

    def __init__(self, url):
        super().__init__(Supervisor.default(url=url))

    async def run(self, workflow, params):
        workflow_context = WorkflowContext(self.supervisor)
        await workflow_context.spawn_workflow(workflow, params)
