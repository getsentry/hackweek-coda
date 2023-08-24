import asyncio

from coda._interest import UpstreamListener
from coda._supervisor import Supervisor
from coda._workflow import WorkflowContext


class Client(UpstreamListener):

    def __init__(self, url):
        super().__init__(Supervisor(url=url))

        # Contains the messages that need to go out but that don't have a response.
        self._outgoing_requests = asyncio.Queue(maxsize=20)

    async def run(self, workflow, params):
        def dispatch(coro):
            self._outgoing_requests.put_nowait(coro)

        workflow_context = WorkflowContext(dispatch, self.supervisor)
        workflow_context.spawn_workflow(workflow, params)

        # For now the client is naively sending out messages but the future implementation would generalize
        # the worker and client into an entity with incoming and outgoing messages.
        while not self._outgoing_requests.empty():
            request_coro = await self._outgoing_requests.get()
            await request_coro

        await self.supervisor.close()
