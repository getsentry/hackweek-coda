import os
import struct
import uuid
from abc import ABC, abstractmethod

import cbor2

from coda.interest import Signal
from coda.utils import generate_uuid


def _default_message_condition(_type, request_id, **kwargs):
    def inner(value):
        message_type = value["type"]
        message_request_id = value.get("request_id")

        return message_type == _type and message_request_id == request_id

    return inner


class SupervisorRequest:

    def __init__(self, cmd, request_id):
        self.cmd = cmd
        self.request_id = request_id


class SupervisorAPI(ABC):

    @abstractmethod
    def make_request(self, cmd, args):
        pass

    @abstractmethod
    def build_condition_for_response(self, request):
        pass

    @abstractmethod
    def get_next_message(self):
        pass

    @abstractmethod
    def extract_response(self, response):
        pass

    async def get_response(self, listener, request):
        signal = Signal()
        listener.listen_for(signal, self.build_condition_for_response(request))
        response = await signal.wait_for_signal()
        return self.extract_response(response)


class MockSupervisorAPI(SupervisorAPI):

    def __init__(self):
        self.messages = [
            {
                "type": "req",
                "cmd": "execute_workflow",
                "args": {
                    "workflow_name": "MyWorkflow",
                    "workflow_run_id": uuid.UUID("fea61924-99f5-45f5-82c1-1082efeaa6af").bytes,
                    "params_id": uuid.UUID("fea61924-99f5-45f5-82c1-1082efeaa6af").bytes,
                },
            },
            {
                "type": "resp",
                "request_id": uuid.UUID("fea61924-99f5-45f5-82c1-1082efeaa6af").bytes,
                "result": 100
            }
        ]
        self.index = 0

    def make_request(self, cmd, args):
        return SupervisorRequest(
            cmd=cmd,
            request_id=uuid.UUID("fea61924-99f5-45f5-82c1-1082efeaa6af").bytes
        )

    def build_condition_for_response(self, request):
        return _default_message_condition(
            _type="resp",
            request_id=request.request_id
        )

    def get_next_message(self):
        if self.index >= len(self.messages):
            return None

        message = self.messages[self.index]
        self.index += 1
        return message

    def extract_response(self, response):
        return response["result"]


class CborSupervisorAPI(SupervisorAPI):

    def __init__(self, url):
        self.url = url
        self.rx = open(os.environ.get("CODA_WORKER_READ_PATH", ""), "rb")
        self.tx = open(os.environ.get("CODA_WORKER_WRITE_PATH", ""), "wb")

    def _write_to_pipe(self, data):
        msg = cbor2.dumps(data)
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

    def make_request(self, cmd, args):
        request_id = generate_uuid().bytes
        request = {
            "type": "req",
            "request_id": request_id,
            "cmd": cmd,
            "args": args,
        }

        self._write_to_pipe(request)
        return SupervisorRequest(cmd, request_id)

    def build_condition_for_response(self, request):
        return _default_message_condition(
            _type="resp",
            request_id=request.request_id,
            cmd=request.cmd
        )

    def get_next_message(self):
        return self._read_from_pipe()

    def extract_response(self, response):
        return response["result"]


class Supervisor:

    def __init__(self, url):
        self._api = MockSupervisorAPI()
        self._listener = None

    async def _make_request_and_wait(self, cmd, args):
        request = self._api.make_request(cmd, args)
        if self._listener is None:
            raise RuntimeError("A listener is required in order to wait for a response")

        response = await self._api.get_response(self._listener, request)
        return response

    def attach_listener(self, listener):
        self._listener = listener

    def consume_next_message(self):
        return self._api.get_next_message()

    def register_worker(self, tasks, workflows):
        self._api.make_request(
            cmd="register_worker",
            args={
                "tasks": tasks,
                "workflows": workflows
            }
        )

    def store_params(self, workflow_run_id, params_id, params):
        self._api.make_request(
            cmd="store_params",
            args={
                "workflow_run_id": workflow_run_id.bytes,
                "params_id": params_id.bytes,
                "params": params,
            }
        )

    async def get_params(self, params_id):
        return await self._make_request_and_wait(
            cmd="get_params",
            args={
                "params_id": params_id.bytes
            }
        )

    def spawn_task(self, task_name, task_id, task_key, params_id, workflow_run_id, persist_result):
        self._api.make_request(
            cmd="spawn_task",
            args={
                "task_name": task_name,
                "task_id": task_id.bytes,
                "task_key": task_key,
                "params_id": params_id.bytes,
                "workflow_run_id": workflow_run_id.bytes,
                "persist_result": persist_result
            }
        )

    async def get_task_result(self, task_key):
        return await self._make_request_and_wait(
            cmd="get_task_result",
            args={
                "task_key": task_key
            })
