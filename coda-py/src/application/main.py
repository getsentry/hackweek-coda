import asyncio
import logging

from application.tasks import sum_two_numbers, divide_by
from application.workflows import my_workflow
from coda.supervisor import Supervisor
from coda.worker import Worker
from coda.workflow import WorkflowContext

logging.basicConfig(level=logging.DEBUG)


async def run():
    supervisor = Supervisor.default()
    worker = Worker(
        supervisor=supervisor,
        tasks=[sum_two_numbers, divide_by],
        workflows=[my_workflow]
    )
    await worker.run()


if __name__ == '__main__':
    asyncio.run(run())
