import asyncio
import logging

from application.workflows import my_workflow
from coda.client import Client

logging.basicConfig(level=logging.DEBUG)


async def run():
    client = Client("127.0.0.1:42069")
    await client.run(my_workflow, {"x": 12})


if __name__ == '__main__':
    asyncio.run(run())
