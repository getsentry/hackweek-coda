import asyncio
from enum import Enum

from coda.workflow import WorkflowContext


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

    def __init__(self, supervisor, tasks, workflows):
        self.supervisor = supervisor
        self.supported_tasks = tasks
        self.supported_workflows = workflows

        # Interests of workflows that are currently suspended because they are waiting for task
        # results.
        self._interests = []
        # Signal used to stop the execution of the main loop from another coroutine.
        self._stop_signal = asyncio.Queue(maxsize=1)

    async def run(self):
        self._register()
        await self._loop()

    def _register(self):
        self.supervisor.api.register_tasks([task.__task_name__ for task in self.supported_tasks])
        self.supervisor.api.register_workflows([workflow.__workflow_name__ for workflow in self.supported_workflows])

    async def _loop(self):
        tasks = []

        while self._is_active():
            message = self.supervisor.api.get_next_message()
            new_task = asyncio.create_task(self._process_message(message))
            tasks.append(new_task)
            # We want to yield, in order to the task to actually run, since we are single threaded.
            await asyncio.sleep(0.0)

        # We want to wait for all signals.
        await asyncio.gather(self._stop_signal.get(), *tasks)

    def _is_active(self):
        return self._stop_signal.empty()

    async def _stop_loop(self):
        await self._stop_signal.put(0)

    async def _process_message(self, message):
        if message is None:
            await self._stop_loop()
            return

        handling_result = await self._handle_message(message)

        # If the worker is told to stop, we immediately break out of the loop.
        if handling_result == MessageHandlingResult.STOP:
            await self._stop_loop()
        elif handling_result == MessageHandlingResult.ERROR:
            raise RuntimeError("An error happened while handling a message")

    async def _handle_message(self, message):
        satisfied = await self._try_to_satisfy_interest(message)
        if satisfied:
            return MessageHandlingResult.SUCCESS

        cmd = message["cmd"]
        if cmd == "hello_worker":
            print("Hi!")
            return MessageHandlingResult.SUCCESS
        elif cmd == "start_workflow":
            return await self._execute_workflow(message["args"])
        elif cmd == "request_worker_shutdown":
            return MessageHandlingResult.STOP

        raise RuntimeError("Message not supported")

    async def _try_to_satisfy_interest(self, message):
        matching_index = None
        matching_interest = None
        # TODO: implement multi-interest matching.
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

    async def _execute_workflow(self, message):
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
        workflow_context = WorkflowContext(self, self.supervisor, workflow_run_id)

        # We fetch the params and run the workflow.
        workflow_params = self.supervisor.api.get_params(params_id)
        result = await found_workflow(workflow_context, **workflow_params)

        print("Result of the workflow is: ", result)

        return MessageHandlingResult.SUCCESS

    def register_interest(self, cmd, condition):
        queue = asyncio.Queue(maxsize=1)

        interest = WorkerInterest(cmd, condition, queue)
        self._interests.append(interest)

        return queue
