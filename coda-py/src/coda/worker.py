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
        await self._register()
        await self._loop()

    def listen_for(self, signal, condition):
        interest = Interest(signal, condition)
        self._interests.append(interest)

    async def _register(self):
        logging.debug(f"Registering {len(self.supported_tasks)} tasks and {len(self.supported_workflows)} workflows")
        await self.supervisor.register_worker(
            tasks=[task.__task_name__ for task in self.supported_tasks],
            workflows=[workflow.__workflow_name__ for workflow in self.supported_workflows]
        )

    async def _loop(self):
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._stop_signal.get())

            while self._is_active():
                message = await self.supervisor.consume_next_message()
                logging.debug(f"Received next message {message}")

                tg.create_task(self._process_message(message), name="ProcessMessage")

                # We want to yield, in order to the task to actually run, since we are single threaded.
                await asyncio.sleep(0.0)

        # We want to wait for all signals.
        logging.debug("Worker stopped")

    def _is_active(self):
        return self._stop_signal.empty()

    async def _stop_loop(self):
        await self._stop_signal.put(0)

    async def _process_message(self, message):
        # In case we don't have a message, ready, we will yield execution to the next coroutine and see if that
        # will make some progress, which will put another message in the input.
        if message is None:
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
        args = message["args"]
        if cmd == "execute_workflow":
            return await self._execute_workflow(args)
        elif cmd == "execute_task":
            return await self._execute_task(args)
        elif cmd == "request_worker_shutdown":
            return MessageHandlingResult.STOP
        else:
            return MessageHandlingResult.NOT_SUPPORTED

    async def _try_to_satisfy_interest(self, message):
        matching_index = None
        matching_interest = None
        logging.debug(f"Trying to satisfy message with the available {len(self._interests)} interests")

        for index, interest in enumerate(self._interests):
            if interest.matches(message):
                logging.debug("Found a matching interest")
                matching_index = index
                matching_interest = interest
                break

        if matching_interest is None:
            logging.debug("No interest found")
            return False

        await matching_interest.satisfy(message)

        logging.debug("Satisfied interest")
        del self._interests[matching_index]

        return True

    async def _execute_workflow(self, args):
        workflow_name = args["workflow_name"]
        workflow_run_id = uuid.UUID(bytes=args["workflow_run_id"])
        params_id = uuid.UUID(bytes=args["params_id"])

        found_workflow = None
        for supported_workflow in self.supported_workflows:
            if supported_workflow.__workflow_name__ == workflow_name:
                found_workflow = supported_workflow

        if found_workflow is None:
            logging.warning(f"Workflow {workflow_name} is not supported in this worker")
            return

        logging.debug(f"Executing workflow {workflow_name}")

        # We register a workflow context, which will encapsulate the logic to drive a workflow.
        workflow_context = WorkflowContext(self.supervisor, workflow_name, workflow_run_id)
        # We fetch the params and run the workflow.
        workflow_params = await self.supervisor.get_params(workflow_run_id, params_id)
        await found_workflow(workflow_context, **workflow_params)

        return MessageHandlingResult.SUCCESS

    async def _execute_task(self, args):
        task_name = args["task_name"]
        # TODO: check how we can use task id.
        task_id = args["task_id"]
        task_key = args["task_key"]
        params_id = uuid.UUID(bytes=args["params_id"])
        workflow_run_id = uuid.UUID(bytes=args["workflow_run_id"])
        persist_result = args["persist_result"]

        found_task = None
        for supported_task in self.supported_tasks:
            if supported_task.__task_name__ == task_name:
                found_task = supported_task

        if found_task is None:
            logging.warning(f"Task {task_name} is not supported in this worker")
            return

        logging.debug(f"Executing task {task_name}")

        # We fetch the params and run the task.
        task_params = await self.supervisor.get_params(workflow_run_id, params_id)
        result = await found_task(**task_params)
        if persist_result:
            logging.debug(f"Persisting result {result} for task {task_name} in workflow {workflow_run_id}")
            await self.supervisor.publish_task_result(task_key, workflow_run_id, result)

        return MessageHandlingResult.SUCCESS
