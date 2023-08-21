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


def write_msg(cmd, args):
    msg = cbor2.dumps({"cmd": cmd, "args": args})
    tx.write(struct.pack('!i', len(msg)) + msg)
    tx.flush()


idx = 0
worker_id = None
try:
    while True:
        msg = read_msg()
        print('<<<', msg)
        if msg["cmd"] == "hello_worker":
            worker_id = uuid.UUID(bytes=msg["args"]["worker_id"])
        if msg["cmd"] == "request_worker_shutdown":
            break
        idx += 1
        if idx == 1:
            write_msg("ping", {})
            write_msg("worker_start", {
                "tasks": ["foo", "bar", "baz"],
                "workflows": ["workflow_foo"],
            })
except KeyboardInterrupt:
    pass
