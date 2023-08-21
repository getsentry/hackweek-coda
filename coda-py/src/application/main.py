import coda_queue as q
import asyncio

from coda_py.workflows import MyWorkflow


async def run():
    # TODO: will be configured from upstream.
    url = "localhost:2233"
    supervisor = q.Supervisor(url)

    worker = q.Worker(tasks=[], workflows=[MyWorkflow])
    await worker.run_with_supervisor(supervisor)


if __name__ == '__main__':
    asyncio.run(run())
