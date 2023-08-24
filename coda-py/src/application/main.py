import asyncio
import logging

import coda
from application.tasks import normalize_event, symbolicate_event, store_event
from application.workflows import process_event

logging.basicConfig(level=logging.DEBUG)


async def run():
    supervisor = coda.Supervisor.default()
    worker = coda.Worker(
        supervisor=supervisor,
        tasks=[normalize_event, symbolicate_event, store_event],
        workflows=[process_event]
    )
    await worker.run()


if __name__ == '__main__':
    asyncio.run(run())
