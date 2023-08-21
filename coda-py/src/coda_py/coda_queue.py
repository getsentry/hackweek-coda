from abc import ABC, abstractmethod
from weakref import ref as weakref
from cbor2 import dumps, loads, load


class Message:
    pass


class ExecuteWorkflow(Message):

    def __init__(self, workflow_name, workflow_params):
        self.workflow_name = workflow_name
        self.workflow_params = workflow_params


class ExecuteTask(Message):

    def __init__(self, workflow_run_id, task_id, task_key, task_params):
        self.workflow_run_id = workflow_run_id
        self.task_id = task_id
        self.task_key = task_key
        self.task_params = task_params


class SupervisorAPI(ABC):

    @abstractmethod
    def get_next_message(self):
        pass

    @abstractmethod
    def register_tasks(self, tasks):
        pass


class CborSupervisorAPI(SupervisorAPI):

    def __init__(self, url):
        self.url = url

    def _make_request(self, cmd, args):
        request = {
            "cmd": cmd,
            "args": args
        }

        serialized_request = dumps(request)
        # TODO: send it upstream through the fifo file.

    def get_next_message(self):
        # TODO: implement reading logic.
        pass

    def register_tasks(self, tasks):
        self._make_request(
            cmd="register_tasks",
            args={
                "tasks": tasks
            }
        )


class Supervisor:

    def __init__(self, url):
        self.api = CborSupervisorAPI(url)

    def get_next_message(self):
        return self.api.get_next_message()

    def register_tasks(self, tasks):
        self.api.register_tasks(tasks)

    def register_workflows(self, workflows):
        pass

# Check if workflow is supported
# Initialize the context which is linked to the supervisor
# Create workflow with a run id and bind it to the context
# Store it in a map keyed by (name, run_id)
#


class Worker:

    def __int__(self, tasks, workflows):
        self.tasks = tasks
        self.workflows = workflows

        self._local_workflows = []

    async def run_with_supervisor(self, supervisor):
        self._register(supervisor)
        self._loop(supervisor)

    def _register(self, supervisor):
        supervisor.register_tasks(self.tasks)
        supervisor.register_workflows(self.workflows)

    def _loop(self, supervisor):
        while True:
            message = supervisor.get_next_message()
            self._handle_message(message)

    def _handle_message(self, message):
        if isinstance(message, ExecuteWorkflow):
            self._execute_workflow()
        elif isinstance(message, ExecuteTask):
            self._execute_task()

        raise RuntimeError("Message not supported")

    def _execute_workflow(self):
        pass

    def _execute_task(self):
        pass


class WorkflowContext:

    def __init__(self, workflow, workflow_run_id, supervisor):
        self.workflow = workflow
        self.supervisor = supervisor