import asyncio
import logging

import coda
from application.workflows import process_event

logging.basicConfig(level=logging.DEBUG)


async def run():
    client = coda.Client("127.0.0.1:42069")
    event_data = {
        "event_id": 1,
        "sdk": "javascript"
    }
    await client.run(process_event, {"project_id": 2, "event_data": event_data})


if __name__ == '__main__':
    asyncio.run(run())
