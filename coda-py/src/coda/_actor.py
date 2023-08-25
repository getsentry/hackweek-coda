import asyncio
import logging
import uuid
from abc import ABC, abstractmethod

from coda._workflow import WorkflowContext


class ActorRxChannel(ABC):

    @abstractmethod
    async def get(self):
        pass


class ActorTxChannel(ABC):

    @abstractmethod
    async def put(self, item):
        pass

    @abstractmethod
    def put_nowait(self, item):
        pass


class QueueChannel(ActorRxChannel, ActorTxChannel):

    def __init__(self, size):
        self._queue = asyncio.Queue(maxsize=size or 0)

    async def get(self):
        return await self._queue.get()

    async def put(self, item):
        await self._queue.put(item)

    def put_nowait(self, item):
        self._queue.put_nowait(item)


class SupervisorChannel(ActorRxChannel):

    def __init__(self, supervisor):
        self._supervisor = supervisor

    async def get(self):
        await self._supervisor.consume_next_message()


class RxActor(ABC):

    def __init__(self, rx_channel, stop_signal):
        self._rx_channel = rx_channel
        self._stop_signal = stop_signal or asyncio.Event()

    async def start(self):
        logging.debug(f"Actor {type(self).__name__} is waiting for incoming messages")
        while not self._stop_signal.is_set():
            item = await self._rx_channel.get()
            await self.on_item_received(item)

    def stop(self):
        self._stop_signal.set()

    @abstractmethod
    async def on_item_received(self, item):
        pass

    @abstractmethod
    async def on_actor_stopped(self):
        pass


class TxActor(RxActor):

    def __init__(self, rx_channel, stop_signal, tx_channel):
        super().__init__(rx_channel, stop_signal)
        self._tx_channel = tx_channel

    async def send(self, item):
        await self._tx_channel.put(item)

    def send_nowait(self, item):
        self._tx_channel.put_nowait(item)


class NonBlockingTxMessagesActor(TxActor):

    def __init__(self, channel, stop_signal):
        super().__init__(channel, stop_signal, channel)

    async def on_item_received(self, item):
        logging.debug("Sending non blocking responseless message")
        # The queue will contain coroutines for fetching remote tasks, even though the ideal solution
        # would be to just have a series of operations that this coroutine will execute on the supervisor.
        await item

    async def on_actor_stopped(self):
        pass


class JobExecutionActor(TxActor):

    def __init__(self, channel, stop_signal, supervisor, supported_workflows, supported_tasks,
                 non_blocking_tx_messages_actor):
        super().__init__(channel, stop_signal, channel)
        self._supervisor = supervisor
        self._supported_workflows = supported_workflows
        self._supported_tasks = supported_tasks
        self._non_blocking_tx_messages_actor = non_blocking_tx_messages_actor
        # We want to keep track of the tasks spawned by this worker so that we can clean them up.
        self._created_tasks = []

    async def on_item_received(self, item):
        job_type, args = item

        # We have to start tasks and not block, since if we block, the processing of jobs will stall and the system
        # will block.
        if job_type == "workflow":
            self._created_tasks.append(asyncio.create_task(self._execute_workflow(args)))
        elif job_type == "task":
            self._created_tasks.append(asyncio.create_task(self._execute_task(args)))
        else:
            raise Exception(f"Job type {job_type} not supported")

    async def on_actor_stopped(self):
        for task in self._created_tasks:
            task.cancel()

        await asyncio.gather(*self._created_tasks)

    async def _execute_workflow(self, args):
        workflow_name = args["workflow_name"]
        workflow_run_id = uuid.UUID(bytes=args["workflow_run_id"])
        params_id = uuid.UUID(bytes=args["params_id"])

        found_workflow = self._supported_workflows.get(workflow_name)
        if found_workflow is None:
            logging.warning(f"Workflow {workflow_name} is not supported in this worker")
            return

        # We register a workflow context, which will encapsulate the logic to drive a workflow.
        workflow_context = WorkflowContext(
            supervisor_dispatch=
            lambda coro: self._non_blocking_tx_messages_actor.send_nowait(coro),
            supervisor=self._supervisor,
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id
        )
        with workflow_context:
            # We fetch the params and run the workflow.
            workflow_params = await self._supervisor.get_params(workflow_run_id, params_id)
            logging.debug(f"Executing workflow {workflow_name} with params {workflow_params}")
            await found_workflow(**workflow_params)
            logging.debug(f"Workflow {workflow_name} finished")

    async def _execute_task(self, args):
        task_name = args["task_name"]
        _ = args["task_id"]
        task_key = args["task_key"]
        params_id = uuid.UUID(bytes=args["params_id"])
        workflow_run_id = uuid.UUID(bytes=args["workflow_run_id"])
        persist_result = args["persist_result"]

        found_task = self._supported_tasks.get(task_name)
        if found_task is None:
            logging.warning(f"Task {task_name} is not supported in this worker")
            return

        # We fetch the params and run the task.
        task_params = await self._supervisor.get_params(workflow_run_id, params_id)
        logging.debug(f"Executing task {task_name} with params {task_params}")
        result = await found_task(**task_params)
        logging.debug(f"Task {task_name} finished with result {result}")

        if persist_result:
            logging.debug(f"Persisting result {result} for task {task_name} in workflow {workflow_run_id}")
            await self._supervisor.publish_task_result(task_key, workflow_run_id, result)
