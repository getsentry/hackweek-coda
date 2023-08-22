import asyncio
import logging

from application.workflows import my_workflow
from coda.supervisor import Supervisor
from coda.worker import Worker

logging.basicConfig(level=logging.DEBUG)


async def run():
    # TODO: will be configured from upstream.
    url = "localhost:2233"
    supervisor = Supervisor(url)

    worker = Worker(supervisor=supervisor, tasks=[], workflows=[my_workflow])
    await worker.run()


if __name__ == '__main__':
    asyncio.run(run())
