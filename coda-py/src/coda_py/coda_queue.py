import uuid
from abc import ABC, abstractmethod
from weakref import ref as weakref
from cbor2 import dumps, loads, load


def coda_workflow(workflow_name):
    def decorator(cls):
        def set_context(self, context):
            self.context = context

        cls.__workflow_name__ = workflow_name
        cls.set_context = set_context

        return cls

    return decorator


def generate_id():
    return uuid.uuid4()


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

    @staticmethod
    def _make_request(cmd, args):
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
        self.supported_tasks = tasks
        self.supported_workflows = workflows

        self._workflows = []

    async def run_with_supervisor(self, supervisor):
        self._register(supervisor)
        self._loop(supervisor)

    def _register(self, supervisor):
        supervisor.register_tasks(self.supported_tasks)
        supervisor.register_workflows(self.supported_workflows)

    def _loop(self, supervisor):
        while True:
            message = supervisor.get_next_message()
            self._handle_message(supervisor, message)

    def _handle_message(self, supervisor, message):
        if isinstance(message, ExecuteWorkflow):
            self._execute_workflow(supervisor, message)
        elif isinstance(message, ExecuteTask):
            self._execute_task()

        raise RuntimeError("Message not supported")

    def _execute_workflow(self, supervisor, message):
        workflow_name = message.workflow_name

        found_workflow = None
        for workflow in self.supported_workflows:
            if workflow.__workflow_name__ == workflow_name:
                found_workflow = workflow

        if found_workflow is None:
            raise RuntimeError(f"Workflow {workflow_name} is not supported in this worker")

        workflow_run_id = generate_id()
        workflow_context = WorkflowContext(workflow_run_id, supervisor)

        workflow = found_workflow()

        found_workflow.set_context(workflow_context)
        found_workflow.run(**message.workflow_params)

    def _execute_task(self, supervisor, message):
        pass


class WorkflowContext:

    def __init__(self, workflow_run_id, supervisor):
        self.workflow_run_id = workflow_run_id
        self.supervisor = supervisor

    def execute_task(self):
        pass