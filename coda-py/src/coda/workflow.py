import logging

from coda.task import TaskHandle
from coda.utils import generate_uuid, hash_cache_key


def coda_workflow(workflow_name):
    def decorator(func):
        func.__workflow_name__ = workflow_name

        return func

    return decorator


class WorkflowContext:

    def __init__(self, supervisor, workflow_name=None, workflow_run_id=None):
        self.supervisor = supervisor
        self.workflow_name = workflow_name
        self.workflow_run_id = workflow_run_id

    async def spawn_task(self, task_function, persistence_key, params):
        task_name = task_function.__task_name__
        task_key = hash_cache_key(
            [self.workflow_run_id, task_name] + list(persistence_key)
        )

        logging.debug(f"Spawning task {task_name} in workflow {self.workflow_name}")

        # We store the parameters of the function as a separate process.
        params_id = generate_uuid()
        await self.supervisor.store_params(
            workflow_run_id=self.workflow_run_id,
            params_id=params_id,
            params=params
        )

        # We spawn the task but in reality the server will see if it already has the task result for the given
        # task key.
        task_id = generate_uuid()
        await self.supervisor.spawn_task(
            task_name=task_name,
            task_id=task_id,
            task_key=task_key,
            params_id=params_id,
            workflow_run_id=self.workflow_run_id,
            persist_result=persistence_key is not None
        )

        # The TaskHandle will be used as a future object that we can await.
        return TaskHandle(
            supervisor=self.supervisor,
            workflow_name=self.workflow_name,
            workflow_run_id=self.workflow_run_id,
            task_id=task_id,
            task_key=task_key
        )

    async def spawn_workflow(self, workflow_function, params):
        workflow_name = workflow_function.__workflow_name__
        workflow_run_id = generate_uuid()

        logging.debug(f"Spawning workflow {workflow_name} in workflow {self.workflow_name}")

        # We store the workflow parameter as a separate process.
        params_id = generate_uuid()
        await self.supervisor.store_params(
            workflow_run_id=workflow_run_id,
            params_id=params_id,
            params=params
        )

        # We spawn the workflow.
        await self.supervisor.spawn_workflow(
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
            params_id=params_id
        )
