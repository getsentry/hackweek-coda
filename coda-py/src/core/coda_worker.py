from enum import Enum

from core.coda_workflow import WorkflowContext


class MessageHandlingResult(Enum):
    SUCCESS = 0
    ERROR = 1
    STOP = 2


class Worker:

    def __init__(self, tasks, workflows):
        self.supported_tasks = tasks
        self.supported_workflows = workflows

        self._active_workflows = {}
        self._active_tasks = {}

    async def run_with_supervisor(self, supervisor):
        self._register(supervisor)
        await self._loop(supervisor)

    def _register(self, supervisor):
        supervisor.api.register_tasks([task.__task_name__ for task in self.supported_tasks])
        supervisor.api.register_workflows([workflow.__workflow_name__ for workflow in self.supported_workflows])

    async def _loop(self, supervisor):
        while True:
            message = supervisor.api.get_next_message()
            handling_result = await self._handle_message(supervisor, message)

            # If the worker is told to stop, we immediately break out of the loop.
            if handling_result == MessageHandlingResult.STOP:
                break
            elif handling_result == MessageHandlingResult.ERROR:
                raise RuntimeError("An error happened while handling a message")

    async def _handle_message(self, supervisor, message):
        cmd = message["cmd"]

        if cmd == "hello_worker":
            print("Hi!")
            return MessageHandlingResult.SUCCESS
        elif cmd == "start_workflow":
            return await self._execute_workflow(supervisor, message["args"])
        elif cmd == "request_worker_shutdown":
            return MessageHandlingResult.STOP

        raise RuntimeError("Message not supported")

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

        workflow_context = WorkflowContext(workflow_run_id, supervisor)

        workflow_instance = found_workflow()
        workflow_instance.set_context(workflow_context)

        # We store active workflows so that we know what is being executed.
        active_workflow = (workflow_name, workflow_run_id)
        self._active_workflows[active_workflow] = workflow_instance

        # We fetch the params and run the workflow.
        workflow_params = supervisor.api.get_params(params_id)
        workflow_instance.run(**workflow_params)

        return MessageHandlingResult.SUCCESS
