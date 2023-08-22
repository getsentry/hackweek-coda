import logging

from coda.task import TaskHandle
from coda.utils import generate_uuid, hash_cache_key


def coda_workflow(workflow_name):
    def decorator(func):
        func.__workflow_name__ = workflow_name

        return func

    return decorator


class WorkflowContext:

    def __init__(self, worker, supervisor, workflow_name, workflow_run_id):
        self.worker = worker
        self.supervisor = supervisor
        self.workflow_name = workflow_name
        self.workflow_run_id = workflow_run_id

    def spawn_task(self, task_function, persistence_key, params):
        task_name = task_function.__name__
        task_id = generate_uuid()
        task_key = hash_cache_key(
            [self.workflow_run_id, task_name] + list(persistence_key)
        )
        logging.info(f"Spawning task {task_name} in workflow {self.workflow_name}")

        # TODO: implement logic to fetch the result.

        params_id = generate_uuid()
        self.supervisor.store_params(
            self.workflow_run_id,
            params_id,
            params
        )

        self.supervisor.spawn_task(
            task_name,
            task_id,
            task_key,
            params_id,
            self.workflow_run_id,
            persistence_key is not None
        )

        # The TaskHandle will be used as a future object that we can await.
        return TaskHandle(task_id, task_key)

    async def await_one(self, task_handle):
        logging.debug(f"Waiting for task {task_handle.task_id} in workflow {self.workflow_name}")
        result = await self.supervisor.get_task_result(task_id=task_handle.task_id, task_key=task_handle.task_key)
        logging.debug(f"Task {task_handle.task_id} finished with result {result}")
        return result
