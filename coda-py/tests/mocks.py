import uuid

import pytest

from coda.supervisor import SupervisorAPI, SupervisorRequest, _default_message_condition, Supervisor
from coda.task import coda_task
from coda.worker import Worker
from coda.workflow import coda_workflow


class MockServer:

    def __init__(self):
        self.tasks = []
        self.workflows = []

        self.active_tasks = {}
        self.active_workflows = {}
        self.active_params = {}

        self.message_queue = []

    def get_next_message(self):
        if len(self.message_queue) == 0:
            return None

        return self.message_queue.pop(0)

    def handle_cmd(self, cmd, args):
        request_id = uuid.uuid4()

        # Forward the call to the actual function.
        func = getattr(self, cmd)
        func(request_id, args)

        return request_id

    def register_worker(self, request_id, args):
        self.tasks = args["tasks"]
        self.workflows = args["workflows"]

        # We want to simulate scheduling the first workflow assuming it's the `MathWorkflow` for now.
        if len(self.workflows) > 0:
            self.execute_math_workflow(request_id, self.workflows[0])

    def execute_math_workflow(self, request_id, workflow_name):
        workflow_run_id = uuid.uuid4()
        params_id = uuid.uuid4()

        self.store_params(None, {"params_id": params_id.bytes, "params": {"a": 10, "b": 20}})

        msg = {
            "type": "req",
            "cmd": "execute_workflow",
            "request_id": request_id,
            "args": {
                "workflow_name": workflow_name,
                "workflow_run_id": workflow_run_id.bytes,
                "params_id": params_id.bytes,
            },
        }

        self.active_workflows[workflow_run_id] = workflow_name
        self.message_queue.append(msg)

    def store_params(self, request_id, args):
        params_id = args["params_id"]
        params = args["params"]

        self.active_params[params_id] = params

    def get_params(self, request_id, args):
        params_id = args["params_id"]

        msg = {
            "type": "resp",
            "cmd": "get_params",
            "request_id": request_id.bytes,
            "result": self.active_params[params_id]
        }

        self.message_queue.append(msg)

    def spawn_task(self, request_id, args):
        pass

    def get_task_result(self, request_id, args):
        pass


class MockSupervisorAPI(SupervisorAPI):

    def __init__(self):
        self.server = MockServer()

    def make_request(self, cmd, args):
        request_id = self.server.handle_cmd(cmd, args)
        return SupervisorRequest(
            cmd=cmd,
            request_id=request_id
        )

    def build_condition_for_response(self, request):
        return _default_message_condition(
            _type="resp",
            request_id=request.request_id
        )

    def get_next_message(self):
        return self.server.get_next_message()

    def extract_response(self, response):
        return response["result"]


@coda_task(task_name="sum_two_numbers")
async def sum_two_numbers(a, b):
    return a + b


@coda_workflow(workflow_name="MathWorkflow")
async def math_workflow(context, a, b):
    task_handle = context.spawn_task(
        sum_two_numbers,
        [a, b],
        {"a": a, "b": b}
    )
    result = await task_handle
    return result
