import asyncio

from application.workflows import MyWorkflow
from core.coda_supervisor import Supervisor
from core.coda_worker import Worker


async def run():
    # TODO: will be configured from upstream.
    url = "localhost:2233"
    supervisor = Supervisor(url)

    worker = Worker(tasks=[], workflows=[MyWorkflow])
    await worker.run_with_supervisor(supervisor)


if __name__ == '__main__':
    asyncio.run(run())
