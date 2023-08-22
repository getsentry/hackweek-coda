from abc import ABC, abstractmethod

import cbor2
import os
import struct


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

    @abstractmethod
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
        self.api = CborSupervisorAPI(url)
