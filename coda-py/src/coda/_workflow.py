import logging
from weakref import ref as weakref

from coda._context import Context
from coda._task import TaskHandle
from coda._utils import generate_uuid, hash_cache_key


def workflow(workflow_name=None):
    def decorator(func):
        workflow = Workflow(
            workflow_name=workflow_name or func.__qualname__,
            func=func
        )
        func.__coda_workflow__ = workflow

        return func

    return decorator


class Workflow:
    def __init__(self, name, func):
        self.workflow_name = name
        self._func = weakref(func)

    def __call__(self, *args, **kwargs):
        return self._func()(*args, **kwargs)


class WorkflowContext(Context):

    def __init__(self, supervisor, workflow_name=None, workflow_run_id=None):
        self.supervisor = supervisor
        self.workflow_name = workflow_name
        self.workflow_run_id = workflow_run_id

    async def spawn_task(self, task_function, args, cache_key=None):
        task_name = task_function.__task_name__
        task_key = hash_cache_key(
            [self.workflow_run_id, task_name] + list(cache_key or [])
        )

        logging.debug(f"Spawning task {task_name} in workflow {self.workflow_name}")

        # TODO:
        # We store the parameters of the function as a separate process.
        params_id = generate_uuid()
        await self.supervisor.store_params(
            workflow_run_id=self.workflow_run_id,
            params_id=params_id,
            params=args
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
            persist_result=cache_key is not None
        )

        # The TaskHandle will be used as a future object that we can await.
        return TaskHandle(
            supervisor=self.supervisor,
            workflow_name=self.workflow_name,
            workflow_run_id=self.workflow_run_id,
            task_id=task_id,
            task_key=task_key
        )

    async def spawn_workflow(self, workflow_function, args):
        workflow_name = workflow_function.__workflow_name__
        workflow_run_id = generate_uuid()

        logging.debug(f"Spawning workflow {workflow_name} in workflow {self.workflow_name}")

        # We store the workflow parameter as a separate process.
        params_id = generate_uuid()
        await self.supervisor.store_params(
            workflow_run_id=workflow_run_id,
            params_id=params_id,
            params=args
        )

        # We spawn the workflow.
        await self.supervisor.spawn_workflow(
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
            params_id=params_id
        )
