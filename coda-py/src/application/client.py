import asyncio
import logging

import coda
from application.workflows import my_workflow

logging.basicConfig(level=logging.DEBUG)


async def run():
    client = coda.Client("127.0.0.1:42069")
    await client.run(my_workflow, {"x": 12})


if __name__ == '__main__':
    asyncio.run(run())
