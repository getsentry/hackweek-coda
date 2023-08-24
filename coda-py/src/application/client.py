import asyncio
import logging

import coda
from application.workflows import process_event

logging.basicConfig(level=logging.DEBUG)


async def run():
    client = coda.Client("127.0.0.1:42069")
    await client.run(process_event, {"x": 12})


if __name__ == '__main__':
    asyncio.run(run())
