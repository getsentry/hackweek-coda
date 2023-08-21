import uuid
from abc import ABC, abstractmethod
import hashlib
import cbor2
import os
import struct


def coda_workflow(workflow_name):
    def decorator(cls):
        def set_context(self, context):
            self.context = context

        cls.__workflow_name__ = workflow_name
        cls.set_context = set_context

        return cls

    return decorator


def coda_task(task_name):
    def decorator(func):
        func.__task_name__ = task_name

        return func

    return decorator


def generate_id():
    return uuid.uuid4()


def hash_cache_key(items):
    h = hashlib.md5()
    for item in items:
        h.update(str(item).encode("utf-8"))
    return h.hexdigest()


class SupervisorAPI(ABC):

    @abstractmethod
    def get_next_message(self):
        pass

    @abstractmethod
    def register_tasks(self, tasks):
        pass

    @abstractmethod
    def register_workflows(self, workflows):
        pass

    @abstractmethod
    def get_params(self, params_id):
        pass


class CborSupervisorAPI(SupervisorAPI):

    def __init__(self, url):
        self.url = url
        self.rx = open(os.environ["CODA_WORKER_READ_PATH"], "rb")
        self.tx = open(os.environ["CODA_WORKER_WRITE_PATH"], "wb")

    def _write_to_pipe(self, cmd, args):
        request = {
            "cmd": cmd,
            "args": args
        }

        msg = cbor2.dumps(request)
        self.tx.write(struct.pack('!i', len(msg)))
        self.tx.write(msg)

    def _read_from_pipe(self):
        msg = self.rx.read(4)
        if not msg:
            return

        bytes_vals = self.rx.read(struct.unpack('!i', msg)[0])
        if not bytes_vals:
            return

        return cbor2.loads(bytes_vals)

    def get_next_message(self):
        return self._read_from_pipe()

    def register_tasks(self, tasks):
        self._write_to_pipe(
            cmd="register_tasks",
            args={
                "tasks": tasks
            }
        )

    def register_workflows(self, workflows):
        self._write_to_pipe(
            cmd="register_workflows",
            args={
                "workflows": workflows
            }
        )

    def get_params(self, params_id):
        pass


class Supervisor:

    def __init__(self, url):
        self.api = CborSupervisorAPI(url)


class Worker:

    def __init__(self, tasks, workflows):
        self.supported_tasks = tasks
        self.supported_workflows = workflows

        self._active_workflows = {}

    async def run_with_supervisor(self, supervisor):
        self._register(supervisor)
        await self._loop(supervisor)

    def _register(self, supervisor):
        supervisor.api.register_tasks([task.__task_name__ for task in self.supported_tasks])
        supervisor.api.register_workflows([workflow.__workflow_name__ for workflow in self.supported_workflows])

    async def _loop(self, supervisor):
        while True:
            message = supervisor.api.get_next_message()
            await self._handle_message(supervisor, message)

    async def _handle_message(self, supervisor, message):
        print(message)
        if message["cmd"] == "start_workflow":
            return await self._execute_workflow(supervisor, message["args"])

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

    async def _execute_task(self, supervisor, message):
        pass


class WorkflowContext:

    def __init__(self, workflow_run_id, supervisor):
        self.workflow_run_id = workflow_run_id
        self.supervisor = supervisor

    def execute_task(self, task_function, persistence_key, params):
        task_name = task_function.__name__
        task_id = generate_id()
        task_key = hash_cache_key(
            [self.workflow_run_id, task_name] + list(persistence_key)
        )

        print(f"Executing task {task_name}")
