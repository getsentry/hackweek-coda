import asyncio
from abc import ABC, abstractmethod


class Signal:

    def __init__(self):
        self._signal = asyncio.Queue(maxsize=1)

    async def send_signal(self, value):
        await self._signal.put(value)

    async def wait_for_signal(self):
        return await self._signal.get()


class Interest:

    def __init__(self, signal, condition):
        self._signal = signal
        self._condition = condition

    def matches(self, value):
        return self._condition(value)

    async def satisfy(self, value):
        await self._signal.send_signal(value)


class Listener(ABC):

    @abstractmethod
    def listen_for(self, signal, condition):
        pass
