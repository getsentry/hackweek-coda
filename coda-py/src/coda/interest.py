import asyncio
import logging
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

    def __init__(self):
        self._interests = []

    async def _check_possible_interests(self, message):
        matching_index = None
        matching_interest = None
        logging.debug(f"Trying to satisfy message with the available {len(self._interests)} interests")

        for index, interest in enumerate(self._interests):
            if interest.matches(message):
                logging.debug("Found a matching interest")
                matching_index = index
                matching_interest = interest
                break

        if matching_interest is None:
            logging.debug("No interest found")
            return False

        await matching_interest.satisfy(message)

        logging.debug("Satisfied interest")
        del self._interests[matching_index]

        return True

    @abstractmethod
    def listen_for(self, signal, condition):
        pass
