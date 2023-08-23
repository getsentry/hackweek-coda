import asyncio
import logging

from application.workflows import my_workflow
from coda.supervisor import Supervisor
from coda.worker import Worker
from coda.workflow import WorkflowContext

logging.basicConfig(level=logging.DEBUG)


async def run():
    supervisor = Supervisor.default()
    worker = Worker(
        supervisor=supervisor,
        tasks=[],
        workflows=[my_workflow]
    )

    async def run_my_workflow():
        context = WorkflowContext(supervisor, "", "")
        context.spawn_workflow(my_workflow, {"x": 10})

    await asyncio.gather(asyncio.create_task(run_my_workflow()))
    await worker.run()


if __name__ == '__main__':
    asyncio.run(run())
