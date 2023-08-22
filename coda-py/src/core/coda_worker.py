import asyncio
from enum import Enum

from core.coda_workflow import WorkflowContext


class MessageHandlingResult(Enum):
    SUCCESS = 0
    ERROR = 1
    STOP = 2


class WorkerInterest:

    def __init__(self, cmd, condition, queue):
        self.cmd = cmd
        self.condition = condition
        self.queue = queue

    def matches(self, message):
        if message["cmd"] != self.cmd:
            return False

        return self.condition(message["args"])

    async def satisfy(self, value):
        await self.queue.put(value)


class Worker:

    def __init__(self, tasks, workflows):
        self.supported_tasks = tasks
        self.supported_workflows = workflows

        self._interests = []
        self._signal_stop = asyncio.Queue(maxsize=1)

    async def run_with_supervisor(self, supervisor):
        self._register(supervisor)
        await self._loop(supervisor)

    def _register(self, supervisor):
        supervisor.api.register_tasks([task.__task_name__ for task in self.supported_tasks])
        supervisor.api.register_workflows([workflow.__workflow_name__ for workflow in self.supported_workflows])

    async def _loop(self, supervisor):
        tasks = []
        while self._signal_stop.empty():
            message = supervisor.api.get_next_message()
            new_task = asyncio.create_task(self._process_message(supervisor, message))
            tasks.append(new_task)

        await asyncio.gather(*tasks)

    async def _process_message(self, supervisor, message):
        handling_result = await self._handle_message(supervisor, message)

        # If the worker is told to stop, we immediately break out of the loop.
        if handling_result == MessageHandlingResult.STOP:
            self._signal_stop.put(0)
        elif handling_result == MessageHandlingResult.ERROR:
            raise RuntimeError("An error happened while handling a message")

    async def _handle_message(self, supervisor, message):
        satisfied = await self._try_to_satisfy_interest(message)
        if satisfied:
            return MessageHandlingResult.SUCCESS

        cmd = message["cmd"]
        if cmd == "hello_worker":
            print("Hi!")
            return MessageHandlingResult.SUCCESS
        elif cmd == "start_workflow":
            return await self._execute_workflow(supervisor, message["args"])
        elif cmd == "request_worker_shutdown":
            return MessageHandlingResult.STOP

        raise RuntimeError("Message not supported")

    async def _try_to_satisfy_interest(self, message):
        matching_index = None
        matching_interest = None
        for index, interest in enumerate(self._interests):
            if interest.matches(message):
                matching_index = index
                matching_interest = interest
                break

        if matching_interest is None:
            return False

        result = message["args"]["result"]
        await matching_interest.satisfy(result)

        del self._interests[matching_index]

        return True

    async def _execute_workflow(self, supervisor, message):
        workflow_name = message["workflow_name"]
        workflow_run_id = message["workflow_run_id"]
        params_id = message["params_id"]

        found_workflow = None
        for supported_workflow in self.supported_workflows:
            if supported_workflow.__workflow_name__ == workflow_name:
                found_workflow = supported_workflow

        if found_workflow is None:
            raise RuntimeError(f"Workflow {workflow_name} is not supported in this worker")

        # We register a workflow context, which will encapsulate the logic to drive a workflow.
        workflow_context = WorkflowContext(self, supervisor, workflow_run_id)

        workflow_instance = found_workflow()
        workflow_instance.set_context(workflow_context)

        # We fetch the params and run the workflow.
        workflow_params = supervisor.api.get_params(params_id)
        await workflow_instance.run(**workflow_params)

        return MessageHandlingResult.SUCCESS

    def register_interest(self, cmd, condition):
        queue = asyncio.Queue(maxsize=1)

        interest = WorkerInterest(cmd, condition, queue)
        self._interests.append(interest)

        return queue
