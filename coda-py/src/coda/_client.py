from coda._interest import Listener
from coda._supervisor import Supervisor
from coda._workflow import WorkflowContext
import asyncio


class Client(Listener):

    def __init__(self, url):
        super().__init__(Supervisor.default(url=url))

        # Contains the messages that need to go out.
        self._outgoing_requests = asyncio.Queue(maxsize=20)

    async def run(self, workflow, params):
        def dispatch(coro):
            self._outgoing_requests.put_nowait(coro)

        workflow_context = WorkflowContext(dispatch, self.supervisor)
        await workflow_context.spawn_workflow(workflow, params)
