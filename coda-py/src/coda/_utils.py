import hashlib
import uuid
import os
import asyncio
import errno
import time


def generate_uuid():
    return uuid.uuid4()


def hash_cache_key(items):
    h = hashlib.md5()

    for item in items:
        h.update(str(item).encode("utf-8"))

    return h.hexdigest()


class NamedPipePair:

    def __init__(
        self,
        in_path,
        out_path,
    ) -> None:
        self._loop = None
        self._in_path = in_path
        self._out_path = out_path
        self._in = None
        self._out = None
        self._fin = None
        self._fout = None
        self._stream_reader = None
        self._stream_writer = None

    async def connect(self, timeout: float = 10.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self._loop is None:
                self._loop = asyncio.get_event_loop()

            if self._in is None:
                self._in = os.open(self._in_path, os.O_RDONLY | os.O_NONBLOCK | os.O_SYNC)

            try:
                if self._out is None:
                    self._out = os.open(self._out_path, os.O_WRONLY | os.O_NONBLOCK)
            except OSError as e:
                if e.errno == errno.ENXIO:
                    await asyncio.sleep(0.1)
                    continue
                raise e

            self._stream_reader = asyncio.StreamReader(loop=self._loop)
            reader_protocol = asyncio.StreamReaderProtocol(self._stream_reader, loop=self._loop)
            self._fin = os.fdopen(self._in, "rb", buffering=0)
            await self._loop.connect_read_pipe(
                lambda: reader_protocol, self._fin
            )

            transport, protocol = await self._loop.connect_write_pipe(
                asyncio.streams.FlowControlMixin, os.fdopen(self._out, "wb", buffering=0))
            self._stream_writer = asyncio.StreamWriter(transport, protocol, None, self._loop)

            return
        raise IOError("failed to connect")

    async def write(self, data) -> None:
        if self._stream_writer is None:
            raise ValueError("not connected")
        self._stream_writer.write(data)
        await self._stream_writer.drain()

    async def read(self, len):
        if self._stream_reader is None:
            raise ValueError("not connected")
        try:
            data = await self._stream_reader.readexactly(len)
            if not data:
                return b""
            return data
        except (asyncio.IncompleteReadError, asyncio.CancelledError):
            return b""

    async def close(self):
        if self._fin is None:
            return
        try:
            self._fin.close()
            self._fout.close()
        except OSError:
            pass
        await asyncio.sleep(0.0)