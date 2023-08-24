import asyncio
import logging
import os
import struct
from abc import ABC, abstractmethod

import cbor2

from coda._interest import Signal
from coda._utils import generate_uuid, NamedPipePair


def _default_message_condition(_type, request_id, **kwargs):
    def inner(value):
        message_type = value["type"]
        # This is supposedly coming in as bytes.
        message_request_id = value.get("request_id")

        return message_type == _type and message_request_id == request_id.bytes

    return inner


class SupervisorRequest:

    def __init__(self, cmd, request_id):
        self.cmd = cmd
        self.request_id = request_id


class SupervisorAPI(ABC):

    @abstractmethod
    async def make_request(self, cmd, args, has_response=False):
        pass

    @abstractmethod
    def build_condition_for_response(self, request):
        pass

    @abstractmethod
    async def get_next_message(self):
        pass

    @abstractmethod
    def extract_response(self, response):
        pass

    @abstractmethod
    async def close(self):
        pass

    async def get_response(self, listener, request):
        signal = Signal()
        listener.listen_for(signal, self.build_condition_for_response(request))
        response = await signal.wait_for_signal()
        return self.extract_response(response)


class CborPipeSupervisorAPI(SupervisorAPI):

    def __init__(self):
        self._pipe = None

    async def _open_pipe(self):
        if self._pipe is not None:
            return self._pipe

        self._pipe = NamedPipePair(
            os.environ["CODA_WORKER_READ_PATH"],
            os.environ["CODA_WORKER_WRITE_PATH"],
        )
        await self._pipe.connect()
        return self._pipe

    async def _write_to_pipe(self, data):
        logging.debug(f"Writing {data} to the write pipe")
        msg = cbor2.dumps(data)

        tx = await self._open_pipe()
        await tx.write(struct.pack('!i', len(msg)) + msg)

    async def _read_from_pipe(self):
        rx = await self._open_pipe()
        msg = await rx.read(4)
        if not msg:
            return None

        bytes_vals = await rx.read(struct.unpack('!i', msg)[0])
        if not bytes_vals:
            return None

        data = cbor2.loads(bytes_vals)
        logging.debug(f"Read {data} from the read pipe")

        return data

    async def make_request(self, cmd, args, has_response=False):
        request = {
            "type": "req",
            "cmd": cmd,
            "args": args,
        }

        request_id = generate_uuid()
        if has_response:
            request["request_id"] = request_id.bytes

        await self._write_to_pipe(request)
        return SupervisorRequest(cmd, request_id)

    def build_condition_for_response(self, request):
        return _default_message_condition(
            _type="resp",
            request_id=request.request_id,
            cmd=request.cmd
        )

    async def get_next_message(self):
        return await self._read_from_pipe()

    def extract_response(self, response):
        return response["result"]

    async def close(self):
        if self._pipe is not None:
            await self._pipe.close()


class CborTCPSupervisorAPI(SupervisorAPI):

    def __init__(self, url):
        self.url = url
        self.rx = None
        self.tx = None

    async def _open_socket(self):
        if self.rx is None and self.tx is None:
            split_url = self.url.split(":")
            rx, tx = await asyncio.open_connection(
                split_url[0], split_url[1]
            )
            self.rx = rx
            self.tx = tx

        return self.rx, self.tx

    async def _write_to_socket(self, data):
        logging.debug(f"Writing {data} to the write socket")
        msg = cbor2.dumps(data)

        _, tx = await self._open_socket()
        tx.write(struct.pack('!i', len(msg)) + msg)
        await tx.drain()

    async def _read_from_socket(self):
        rx, _ = await self._open_socket()

        msg = await rx.read(4)
        if not msg:
            return None

        bytes_vals = await rx.read()
        if not bytes_vals:
            return None

        data = cbor2.loads(bytes_vals)
        logging.debug(f"Read {data} from the read pipe")

        return data

    async def make_request(self, cmd, args, has_response=False):
        request = {
            "type": "req",
            "cmd": cmd,
            "args": args,
        }

        request_id = generate_uuid()
        if has_response:
            request["request_id"] = request_id.bytes

        await self._write_to_socket(request)
        return SupervisorRequest(cmd, request_id)

    def build_condition_for_response(self, request):
        return _default_message_condition(
            _type="resp",
            request_id=request.request_id,
            cmd=request.cmd
        )

    async def get_next_message(self):
        return await self._read_from_socket()

    def extract_response(self, response):
        return response["result"]

    async def close(self):
        if self.tx is not None:
            self.tx.close()
            await self.tx.wait_closed()


class Supervisor:

    def __init__(self, api=None):
        self._api = api
        self._listener = None

    @classmethod
    def default(cls, url=None):
        if url is None:
            return cls(api=CborPipeSupervisorAPI())

        return cls(api=CborTCPSupervisorAPI(url))

    async def _make_request_and_wait(self, cmd, args):
        request = await self._api.make_request(cmd, args, True)
        if self._listener is None:
            raise RuntimeError("A listener is required in order to wait for a response")

        response = await self._api.get_response(self._listener, request)
        return response

    def attach_listener(self, listener):
        self._listener = listener

    async def close(self):
        await self._api.close()

    async def consume_next_message(self):
        return await self._api.get_next_message()

    async def register_worker(self, tasks, workflows):
        await self._api.make_request(
            cmd="register_worker",
            args={
                "tasks": tasks,
                "workflows": workflows
            }
        )

    async def store_params(self, workflow_run_id, params_id, params):
        await self._api.make_request(
            cmd="store_params",
            args={
                "workflow_run_id": workflow_run_id.bytes,
                "params_id": params_id.bytes,
                "params": params,
            }
        )

    async def get_params(self, workflow_run_id, params_id):
        return await self._make_request_and_wait(
            cmd="get_params",
            args={
                "workflow_run_id": workflow_run_id.bytes,
                "params_id": params_id.bytes
            }
        )

    async def spawn_task(self, task_name, task_id, task_key, params_id, workflow_run_id, persist_result):
        await self._api.make_request(
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

    async def publish_task_result(self, task_key, workflow_run_id, result):
        await self._api.make_request(
            cmd="publish_task_result",
            args={
                "task_key": task_key,
                "workflow_run_id": workflow_run_id.bytes,
                "result": result
            }
        )

    async def get_task_result(self, workflow_run_id, task_key):
        return await self._make_request_and_wait(
            cmd="get_task_result",
            args={
                "workflow_run_id": workflow_run_id.bytes,
                "task_key": task_key
            }
        )

    async def spawn_workflow(self, workflow_name, workflow_run_id, params_id):
        await self._api.make_request(
            cmd="spawn_workflow",
            args={
                "workflow_name": workflow_name,
                "workflow_run_id": workflow_run_id.bytes,
                "params_id": params_id.bytes
            }
        )

    async def workflow_ended(self, workflow_run_id):
        await self._api.make_request(
            cmd="workflow_ended",
            args={
                "workflow_run_id": workflow_run_id.bytes
            }
        )
