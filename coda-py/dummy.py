import os
import uuid
import cbor2
import struct

rx = open(os.environ["CODA_WORKER_READ_PATH"], "rb")
tx = open(os.environ["CODA_WORKER_WRITE_PATH"], "wb")


def read_msg():
    msg = rx.read(4)
    if not msg:
        raise Exception('did not read header')
    bytes = rx.read(struct.unpack('!i', msg)[0])
    if not bytes:
        raise Exception('did not read payload')
    return cbor2.loads(bytes)


def send_msg(data):
    msg = cbor2.dumps(data)
    tx.write(struct.pack('!i', len(msg)) + msg)
    tx.flush()


workflow_run_id = uuid.uuid4()
params_id = uuid.uuid4()

idx = 0
worker_id = None
task = None
try:
    while True:
        if idx == 0:
            send_msg({
                "type": "req",
                "cmd": "register_worker",
                "args": {
                    "tasks": ["symbolicate", "bar", "baz"],
                    "workflows": ["workflow_foo"],
                }
            })
            send_msg({
                "type": "req",
                "cmd": "store_params",
                "args": {
                    "workflow_run_id": workflow_run_id.bytes,
                    "params_id": params_id.bytes,
                    "params": {"foo": "bar"},
                }
            })
            send_msg({
                "type": "req",
                "cmd": "spawn_task",
                "args": {
                    "task_name": "symbolicate",
                    "task_id": uuid.uuid4().bytes,
                    "task_key": uuid.uuid4().bytes,
                    "params_id": params_id.bytes,
                    "workflow_run_id": workflow_run_id.bytes,
                    "persist_result": True,
                }
            })
        msg = read_msg()
        print('<<<', msg)
        if msg["type"] == "req" and msg["cmd"] == "execute_task":
            task = {
                "workflow_run_id": msg["args"]["workflow_run_id"],
                "task_key": msg["args"]["task_key"],
            }
            send_msg({
                "type": "req",
                "cmd": "get_params",
                "request_id": uuid.uuid4().bytes,
                "args": {
                    "workflow_run_id": msg["args"]["workflow_run_id"],
                    "params_id": msg["args"]["params_id"],
                }
            })
        elif msg["type"] == "resp":
            params = msg["result"]
            send_msg({
                "type": "req",
                "cmd": "publish_task_result",
                "args": {
                    "result": "whatever",
                    **task
                }
            })
        idx += 1
except KeyboardInterrupt:
    pass
