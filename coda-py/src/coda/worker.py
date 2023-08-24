import asyncio
import logging
import uuid
from enum import Enum

from coda.interest import Listener
from coda.workflow import WorkflowContext


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
        self._supported_tasks = {task.__task_name__: task for task in tasks}
        self._supported_workflows = {workflow.__workflow_name__: workflow for workflow in workflows}

        # Signal used to stop the execution of the main loop from another coroutine.
        self._stop_signal = asyncio.Queue(maxsize=1)

    async def run(self):
        await self._register()
        await self._loop()

    async def _register(self):
        logging.debug(f"Registering {len(self._supported_tasks)} tasks and {len(self._supported_workflows)} workflows")
        await self.supervisor.register_worker(
            tasks=list(self._supported_tasks.keys()),
            workflows=list(self._supported_workflows.keys())
        )

    async def _loop(self):
        # We want to wait for all signals.
        async with asyncio.TaskGroup() as tg:
            while self._is_active():
                message = await self.supervisor.consume_next_message()
                logging.debug(f"Received next message {message}")

                tg.create_task(self._process_message(message), name=_name_from_message(message))

                # We want to yield, in order to the task to actually run, since we are single threaded.
                await asyncio.sleep(0.0)

        # We make sure that the worker was told to be stopped.
        reason = await self._stop_signal.get()
        logging.debug(f"Worker stopped for reason: {reason}")

    def _is_active(self):
        return self._stop_signal.empty()

    async def _stop_loop(self, reason=""):
        await self._stop_signal.put(reason)

    async def _process_message(self, message):
        if message is None:
            await self._stop_loop(reason="Received None message from the supervisor")
            return

        handling_result = await self._handle_message(message)

        # If the worker is told to stop, we immediately break out of the loop.
        if handling_result == MessageHandlingResult.STOP:
            await self._stop_loop(reason="Received a shutdown message from the supervisor")
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

        # We register a workflow context, which will encapsulate the logic to drive a workflow.
        workflow_context = WorkflowContext(self.supervisor, workflow_name, workflow_run_id)
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
