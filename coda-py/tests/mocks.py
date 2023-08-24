import uuid

import coda
import pytest

from coda._supervisor import SupervisorAPI, SupervisorRequest, _default_message_condition


class MockServer:

    def __init__(self):
        self.tasks = []
        self.workflows = []

        self.active_tasks = {}
        self.active_workflows = {}

        self.stored_params = {}
        self.stored_tasks_results = {}

        self.pending_get_task_result_reqs = {}

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

        self.store_params(None, {"params_id": params_id.bytes, "params": {"x": 5}})

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

        self.stored_params[params_id] = params

    def get_params(self, request_id, args):
        params_id = args["params_id"]

        msg = {
            "type": "resp",
            "cmd": "get_params",
            "request_id": request_id.bytes,
            "result": self.stored_params[params_id]
        }

        self.message_queue.append(msg)

    def spawn_task(self, request_id, args):
        task_name = args["task_name"]
        task_id = args["task_id"]
        task_key = args["task_key"]
        params_id = args["params_id"]
        workflow_run_id = args["workflow_run_id"]
        persist_result = args["persist_result"]

        request_id = uuid.uuid4()

        msg = {
            "type": "req",
            "cmd": "execute_task",
            "request_id": request_id.bytes,
            "args": {
                "task_name": task_name,
                "task_id": task_id,
                "task_key": task_key,
                "params_id": params_id,
                "workflow_run_id": workflow_run_id,
                "persist_result": persist_result
            }
        }

        self.message_queue.append(msg)
        self.active_tasks[(task_id, task_key)] = task_name

    def publish_task_result(self, request_id, args):
        task_key = args["task_key"]
        result = args["result"]

        request_id_to_unblock = self.pending_get_task_result_reqs.get(task_key)
        if request_id_to_unblock is None:
            return

        msg = {
            "type": "resp",
            "cmd": "get_task_result",
            "request_id": request_id_to_unblock.bytes,
            "result": result
        }

        self.message_queue.append(msg)
        del self.pending_get_task_result_reqs[task_key]

    def get_task_result(self, request_id, args):
        task_key = args["task_key"]

        if task_key not in self.stored_tasks_results:
            # We store the request id, in order to wake up the correct interest on task result publishing.
            self.pending_get_task_result_reqs[task_key] = request_id
            return

        msg = {
            "type": "resp",
            "cmd": "get_task_result",
            "request_id": request_id.bytes,
            "result": self.stored_tasks_results[task_key]
        }

        self.message_queue.append(msg)
        del self.stored_tasks_results[task_key]


class MockSupervisorAPI(SupervisorAPI):

    def __init__(self):
        self.server = MockServer()

    def make_request(self, cmd, args, has_response):
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


@coda.task(task_name="sum_two_numbers")
async def sum_two_numbers(a, b):
    return a + b


@coda.workflow(workflow_name="MathWorkflow")
async def math_workflow(context, x):
    a = x * 10
    b = x * 100

    task_handle = context.spawn_task(
        sum_two_numbers,
        [a, b],
        {"a": a, "b": b}
    )
    result = await task_handle
    print(f"The computation finished with result {result}")
