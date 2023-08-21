import os
import cbor2
import struct

rx = open(os.environ["CODA_WORKER_READ_PATH"], "rb")
tx = open(os.environ["CODA_WORKER_WRITE_PATH"], "wb")


def read_msg():
    msg = rx.read(4)
    if not msg:
        return
    bytes = rx.read(struct.unpack('!i', msg)[0])
    if not bytes:
        return
    return cbor2.loads(bytes)



print(read_msg())
