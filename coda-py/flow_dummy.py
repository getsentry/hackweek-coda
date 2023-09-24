import asyncio
import struct
import uuid

import cbor2


class TCPFlow:

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

    async def write_to_socket(self, data):
        msg = cbor2.dumps(data)

        _, tx = await self._open_socket()
        tx.write(struct.pack('!i', len(msg)) + msg)
        await tx.drain()

    async def read_from_socket(self):
        rx, _ = await self._open_socket()

        msg = await rx.read(4)
        if not msg:
            return None

        bytes_vals = await rx.read()
        if not bytes_vals:
            return None

        return cbor2.loads(bytes_vals)


async def run():
    tcp = TCPFlow("127.0.0.1:56019")
    spawn_workflow = {
        "type": "req",
        "cmd": "spawn_workflow",
        "args": {
            "workflow_name": "MyWorkflow",
            "workflow_run_id": uuid.uuid4().bytes,
            "params_id": uuid.uuid4().bytes,

        }
    }
    await tcp.write_to_socket(spawn_workflow)


if __name__ == '__main__':
    asyncio.run(run())
