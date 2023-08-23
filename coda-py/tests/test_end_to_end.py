import pytest

from coda.supervisor import Supervisor
from coda.worker import Worker
from mocks import MockSupervisorAPI, sum_two_numbers, math_workflow

pytest_plugins = ('pytest_asyncio',)


@pytest.fixture
def mock_math_worker():
    api = MockSupervisorAPI()
    supervisor = Supervisor(api)
    worker = Worker(
        supervisor=supervisor,
        tasks=[sum_two_numbers],
        workflows=[math_workflow]
    )
    return worker


@pytest.mark.asyncio
async def test_simple(mock_math_worker):
    await mock_math_worker.run()
