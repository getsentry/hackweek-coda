import asyncio
import logging
import uuid
from enum import Enum

from coda._interest import Listener
from coda._workflow import WorkflowContext


def _name_from_message(message):
    suffix = "none"
    if message is not None:
        suffix = message.get("cmd", suffix)

    return f"ProcessMessage-{suffix}"


class MessageHandlingResult(Enum):
    SUCCESS = 0
    ERROR = 1
    STOP = 2
    NOT_SUPPORTED = 3


class Worker(Listener):

    def __init__(self, supervisor, tasks, workflows):
        super().__init__(supervisor)
        self._supported_tasks = {
            t.__coda_task__.task_name: t.__coda_task__
            for t in tasks
        }
        self._supported_workflows = {
            w.__coda_workflow__.workflow_name: w.__coda_workflow__
            for w in workflows
        }

        # Contains the messages that need to go out.
        self._outgoing_requests = asyncio.Queue(maxsize=20)
        # Signals the main loop that the worker needs to shut down.
        self._shutdown_event = asyncio.Event()

    async def run(self):
        await self._register()
        await self._main_loop()

    async def _register(self):
        logging.debug(f"Registering {len(self._supported_tasks)} tasks and {len(self._supported_workflows)} workflows")
        await self.supervisor.register_worker(
            tasks=list(self._supported_tasks.keys()),
            workflows=list(self._supported_workflows.keys())
        )

    async def _main_loop(self):
        # We want to wait for all signals.
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._process_outgoing_messages(), name="ProcessOutgoingMessages")
            tg.create_task(self._process_incoming_messages(), name="ProcessIncomingMessages")

        # We make sure that the worker was told to be shutdown.
        await self._shutdown_event.wait()
        logging.debug(f"Worker shutdown")

    def _shutdown_worker(self):
        return self._shutdown_event.set()

    def _is_shutdown(self):
        return self._shutdown_event.is_set()

    async def _process_outgoing_messages(self):
        while not self._is_shutdown():
            request_coro = await self._outgoing_requests.get()
            # The queue will contain coroutines for fetching remote tasks.
            await request_coro

    async def _process_incoming_messages(self):
        while not self._is_shutdown():
            message = await self.supervisor.consume_next_message()
            if message is None:
                self._shutdown_worker()
                return

            handling_result = await self._handle_message(message)

            # If the worker is told to stop, we immediately break out of the loop.
            if handling_result == MessageHandlingResult.STOP:
                self._shutdown_worker()
            elif handling_result == MessageHandlingResult.NOT_SUPPORTED:
                logging.warning(f"Message {message} is not supported")
            elif handling_result == MessageHandlingResult.ERROR:
                raise RuntimeError(f"An error occurred while handling the message {message}")

    async def _handle_message(self, message):
        if await self._check_possible_interests(message):
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

    async def _execute_workflow(self, args):
        workflow_name = args["workflow_name"]
        workflow_run_id = uuid.UUID(bytes=args["workflow_run_id"])
        params_id = uuid.UUID(bytes=args["params_id"])

        found_workflow = self._supported_workflows.get(workflow_name)
        if found_workflow is None:
            logging.warning(f"Workflow {workflow_name} is not supported in this worker")
            return

        def dispatch(coro):
            self._outgoing_requests.put_nowait(coro)

        # We register a workflow context, which will encapsulate the logic to drive a workflow.
        workflow_context = WorkflowContext(dispatch, workflow_name, workflow_run_id)
        with workflow_context:
            # We fetch the params and run the workflow.
            workflow_params = await self.supervisor.get_params(workflow_run_id, params_id)
            logging.debug(f"Executing workflow {workflow_name}")
            await found_workflow(**workflow_params)

        return MessageHandlingResult.SUCCESS

    async def _execute_task(self, args):
        task_name = args["task_name"]
        # TODO: check how we can use task id.
        task_id = args["task_id"]
        task_key = args["task_key"]
        params_id = uuid.UUID(bytes=args["params_id"])
        workflow_run_id = uuid.UUID(bytes=args["workflow_run_id"])
        persist_result = args["persist_result"]

        found_task = self._supported_tasks.get(task_name)
        if found_task is None:
            logging.warning(f"Task {task_name} is not supported in this worker")
            return

        # We fetch the params and run the task.
        task_params = await self.supervisor.get_params(workflow_run_id, params_id)
        logging.debug(f"Executing task {task_name}")
        result = await found_task(**task_params)
        logging.debug(f"Task {task_name} finished with result {result}")

        if persist_result:
            logging.debug(f"Persisting result {result} for task {task_name} in workflow {workflow_run_id}")
            await self.supervisor.publish_task_result(task_key, workflow_run_id, result)

        return MessageHandlingResult.SUCCESS
