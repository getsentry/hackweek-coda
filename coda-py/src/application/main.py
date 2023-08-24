import asyncio
import logging

from application.tasks import sum_two_numbers
from application.workflows import my_workflow
from coda.supervisor import Supervisor
from coda.worker import Worker
from coda.workflow import WorkflowContext

logging.basicConfig(level=logging.DEBUG)


async def run():
    supervisor = Supervisor.default()
    worker = Worker(
        supervisor=supervisor,
        tasks=[sum_two_numbers],
        workflows=[my_workflow]
    )

    async def run_my_workflow():
        await asyncio.sleep(1.0)
        context = WorkflowContext(supervisor, "", "")
        await context.spawn_workflow(my_workflow, {"x": 10})

    async def delayed_run():
        await worker.run()

    async with asyncio.TaskGroup() as tg:
        tg.create_task(delayed_run())
        tg.create_task(run_my_workflow())


if __name__ == '__main__':
    asyncio.run(run())
