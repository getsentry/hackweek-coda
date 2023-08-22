import asyncio
import logging
import uuid
from enum import Enum

from coda.interest import Interest, Listener
from coda.workflow import WorkflowContext


class MessageHandlingResult(Enum):
    SUCCESS = 0
    ERROR = 1
    STOP = 2
    NOT_SUPPORTED = 3


class Worker(Listener):

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
        self.supervisor.attach_listener(self)
        self._register()
        await self._loop()

    def _register(self):
        logging.debug(f"Registering {len(self.supported_tasks)} tasks and {len(self.supported_workflows)} workflows")
        self.supervisor.register_worker(
            tasks=[task.__task_name__ for task in self.supported_tasks], 
            workflows=[workflow.__workflow_name__ for workflow in self.supported_tasks]
        )

    async def _loop(self):
        tasks = []

        while self._is_active():
            message = self.supervisor.consume_next_message()
            logging.debug(f"Received next message {message}")
            task_name = "ProcessMessage"
            new_task = asyncio.create_task(self._process_message(message), name=task_name)
            tasks.append(new_task)
            # We want to yield, in order to the task to actually run, since we are single threaded.
            await asyncio.sleep(0.0)

        logging.debug("Collecting all pending tasks before stopping")
        for task in tasks:
            if not task.done():
                logging.debug(f"Task {task.get_name()} still running")

        # We want to wait for all signals.
        await asyncio.gather(self._stop_signal.get(), *tasks)
        logging.debug("Worker stopped")

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
        elif handling_result == MessageHandlingResult.NOT_SUPPORTED:
            logging.warning(f"Message {message} is not supported")
        elif handling_result == MessageHandlingResult.ERROR:
            raise RuntimeError(f"An error occurred while handling the message {message}")

    async def _handle_message(self, message):
        satisfied = await self._try_to_satisfy_interest(message)
        if satisfied:
            return MessageHandlingResult.SUCCESS

        cmd = message["cmd"]
        if cmd == "execute_workflow":
            return await self._execute_workflow(message["args"])
        elif cmd == "request_worker_shutdown":
            return MessageHandlingResult.STOP
        else:
            return MessageHandlingResult.NOT_SUPPORTED

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

        await matching_interest.satisfy(message)

        del self._interests[matching_index]

        return True

    async def _execute_workflow(self, message):
        workflow_name = message["workflow_name"]
        workflow_run_id = uuid.UUID(bytes=message["workflow_run_id"])
        params_id = uuid.UUID(bytes=message["params_id"])

        found_workflow = None
        for supported_workflow in self.supported_workflows:
            if supported_workflow.__workflow_name__ == workflow_name:
                found_workflow = supported_workflow

        if found_workflow is None:
            raise RuntimeError(f"Workflow {workflow_name} is not supported in this worker")

        # We register a workflow context, which will encapsulate the logic to drive a workflow.
        workflow_context = WorkflowContext(self, self.supervisor, workflow_name, workflow_run_id)

        # We fetch the params and run the workflow.
        workflow_params = await self.supervisor.get_params(params_id)
        result = await found_workflow(workflow_context, **workflow_params)
        logging.debug(f"The workflow {workflow_name} terminated with result {result}")

        return MessageHandlingResult.SUCCESS

    def listen_for(self, signal, condition):
        interest = Interest(signal, condition)
        self._interests.append(interest)
