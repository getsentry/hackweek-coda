import asyncio
import logging
from enum import Enum

from coda._actor import NonBlockingTxMessagesActor, JobExecutionActor, QueueChannel
from coda._interest import UpstreamListener
from coda._supervisor import Supervisor

EXECUTE_CMD_TO_JOB_TYPE = {
    "execute_workflow": "workflow",
    "execute_task": "task"
}


class MessageHandlingResult(Enum):
    SUCCESS = 0
    ERROR = 1
    STOP = 2
    NOT_SUPPORTED = 3


class Worker(UpstreamListener):

    def __init__(self, tasks, workflows, supervisor=None):
        if supervisor is None:
            supervisor = Supervisor()
        super().__init__(supervisor)
        # Configures the supported tasks and workflows.
        self._supported_tasks = {
            t.__coda_task__.task_name: t
            for t in tasks
        }
        self._supported_workflows = {
            w.__coda_workflow__.workflow_name: w
            for w in workflows
        }
        # Signals the main loop that the worker needs to shut down.
        self._shutdown_signal = asyncio.Event()
        # Actor responsible for forwarding non-blocking responseless messages.
        self._non_blocking_tx_messages_actor = NonBlockingTxMessagesActor(
            channel=QueueChannel(size=100),
            stop_signal=self._shutdown_signal
        )
        # Actor responsible for handling the execution of jobs.
        self._job_execution_actor = JobExecutionActor(
            channel=QueueChannel(size=100),
            stop_signal=self._shutdown_signal,
            supervisor=self.supervisor,
            supported_tasks=self._supported_tasks,
            supported_workflows=self._supported_workflows,
            non_blocking_tx_messages_actor=self._non_blocking_tx_messages_actor
        )

    async def run(self):
        await self._handshake()
        await self._main_loop()

    def _shutdown_worker(self):
        return self._shutdown_signal.set()

    def _is_shutdown(self):
        return self._shutdown_signal.is_set()

    async def _handshake(self):
        logging.debug(f"Registering {len(self._supported_tasks)} tasks and {len(self._supported_workflows)} workflows")
        await self.supervisor.register_worker(
            tasks=list(self._supported_tasks.keys()),
            workflows=list(self._supported_workflows.keys())
        )

    async def _main_loop(self):
        # We will wait for all the tasks in the group to finish (e.g., when the shutdown event is generated).
        async with asyncio.TaskGroup() as tg:
            # We start the coroutine for handling the consumed message.
            tg.create_task(self._process_consumed_message(), name="ProcessConsumedMessages")
            # We start the actors responsible for handling a subset of the worker's capabilities.
            tg.create_task(self._job_execution_actor.start(), name="JobExecutionActor")
            tg.create_task(self._non_blocking_tx_messages_actor.start(), name="NonBlockingTxMessagesActor")

        logging.debug("Worker shutting down")
        # We make sure that the worker was told to be shutdown.
        await self._shutdown_signal.wait()
        # We close the connection to the supervisor.
        await self.supervisor.close()
        logging.debug("Worker shutdown")

    async def _process_consumed_message(self):
        while not self._is_shutdown():
            message = await self.supervisor.consume_next_message()
            await self._process_message(message)

    async def _process_message(self, message):
        logging.debug(f"Received message {message}")
        if message is None:
            self._shutdown_worker()
            return

        handling_result = await self._handle_message(message)
        if handling_result == MessageHandlingResult.STOP:
            self._shutdown_worker()
        elif handling_result == MessageHandlingResult.NOT_SUPPORTED:
            logging.warning(f"Message {message} is not supported")
        elif handling_result == MessageHandlingResult.ERROR:
            raise Exception(f"An error occurred while handling the message {message}")

    async def _handle_message(self, message):
        # We first check if there are active interests in this message.
        if await self._check_possible_interests(message):
            return MessageHandlingResult.SUCCESS

        # If there are no active interests, we will try to see if we can execute the message.
        cmd = message["cmd"]
        args = message["args"]
        if (job_type := EXECUTE_CMD_TO_JOB_TYPE.get(cmd)) is not None:
            # TODO: can deadlock if the queue is full, since this coroutine will stall and no messages will
            #   be consumed which are required by another coroutine to continue.
            await self._job_execution_actor.send((job_type, args))
            return MessageHandlingResult.SUCCESS
        elif cmd == "request_worker_shutdown":
            return MessageHandlingResult.STOP

        return MessageHandlingResult.NOT_SUPPORTED
