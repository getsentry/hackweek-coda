import os
import struct
from abc import ABC, abstractmethod

import cbor2


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
    def store_params(self, workflow_run_id, params_id, params):
        pass

    @abstractmethod
    def get_params(self, params_id):
        pass

    @abstractmethod
    def spawn_task(self, task_name, task_id, task_key, params_id, workflow_run_id, persist_result):
        pass


class MockSupervisorAPI(SupervisorAPI):

    def __init__(self):
        self.messages = [
            {
                "cmd": "start_workflow",
                "args": {
                    "workflow_name": "MyWorkflow",
                    "workflow_run_id": 10,
                    "params_id": 1
                }
            },
            {
                "cmd": "publish_task_result",
                "args": {
                    "task_id": 10,
                    "result": 100
                }
            }
        ]
        self.index = 0

    def get_next_message(self):
        if self.index >= len(self.messages):
            return None

        message = self.messages[self.index]
        self.index += 1

        return message

    def register_tasks(self, tasks):
        pass

    def register_workflows(self, workflows):
        pass

    def store_params(self, workflow_run_id, params_id, params):
        pass

    def get_params(self, params_id):
        return {
            "a": 10,
            "b": 20
        }

    def spawn_task(self, task_name, task_id, task_key, params_id, workflow_run_id, persist_result):
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

    async def get_next_message(self):
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

    def store_params(self, workflow_run_id, params_id, params):
        self._write_to_pipe(
            cmd="store_params",
            args={
                "workflow_run_id": workflow_run_id,
                "params_id": params_id,
                "params": params
            }
        )

    def get_params(self, params_id):
        self._write_to_pipe(
            cmd="get_params",
            args={
                "params_id": params_id
            }
        )

    def spawn_task(self, task_name, task_id, task_key, params_id, workflow_run_id, persist_result):
        self._write_to_pipe(
            cmd="spawn_task",
            args={
                "task_name": task_name,
                "task_id": task_id,
                "task_key": task_key,
                "params_id": params_id,
                "workflow_run_id": workflow_run_id,
                "persist_result": persist_result
            }
        )


class Supervisor:

    def __init__(self, url):
        # TODO: change later when you want to use the actual protocol.
        self.api = MockSupervisorAPI()
