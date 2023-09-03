import asyncio
import os
import uuid
import cbor2
import struct

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

    async def _write_to_socket(self, data):
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

        return cbor2.loads(bytes_vals)