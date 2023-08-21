import os
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


while True:
    msg = read_msg()
    print(msg)
    if msg["cmd"] == "request_worker_shutdown":
        break
