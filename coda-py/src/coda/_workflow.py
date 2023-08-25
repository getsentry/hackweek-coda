import logging
from weakref import ref as weakref

from coda._context import Context
from coda._task import TaskHandle
from coda._utils import generate_uuid, hash_cache_key, get_object_name


def workflow(workflow_name=None):
    def decorator(func):
        inner_workflow = Workflow(
            workflow_name=workflow_name or get_object_name(func),
            func=func
        )
        func.__coda_workflow__ = inner_workflow

        return func

    return decorator


class Workflow:
    def __init__(self, workflow_name, func):
        self.workflow_name = workflow_name
        self._func = weakref(func)

    def __call__(self, *args, **kwargs):
        return self._func()(*args, **kwargs)


class WorkflowContext(Context):

    def __init__(self, supervisor_dispatch, supervisor, workflow_name=None, workflow_run_id=None):
        # The dispatch will accept a coroutine, which is fine in principle but only iff they will be guaranteed
        # to be executed on the same event loop.
        self._supervisor_dispatch = supervisor_dispatch
        # The supervisor is passed just to have the coroutine generation here but for the future it should be refactored
        # to just have a data schema for dispatching requests.
        self._supervisor = supervisor
        self._workflow_name = workflow_name
        self._workflow_run_id = workflow_run_id

    def spawn_task(self, task_function, args, cache_key=None):
        coda_task = task_function.__coda_task__
        if hasattr(task_function, "__coda_task__"):
            task_name = coda_task.task_name
        else:
            task_name = task_function
        task_key = hash_cache_key(
            [self._workflow_run_id, task_name] + list(cache_key or [])
        )
        retries_remaining = coda_task.max_retries

        logging.debug(f"Spawning task {task_name} in workflow {self._workflow_name}")

        # We store the parameters of the function as a separate process.
        params_id = generate_uuid()
        store_params = self._supervisor.store_params(
            workflow_run_id=self._workflow_run_id,
            params_id=params_id,
            params=args
        )
        self._supervisor_dispatch(store_params)

        # We spawn the task but in reality the server will see if it already has the task result for the given
        # task key.
        task_id = generate_uuid()
        spawn_task = self._supervisor.spawn_task(
            task_name=task_name,
            task_id=task_id,
            task_key=task_key,
            params_id=params_id,
            workflow_run_id=self._workflow_run_id,
            persist_result=cache_key is not None,
            retries_remaining=retries_remaining
        )
        self._supervisor_dispatch(spawn_task)

        # The TaskHandle will be used as a future object that we can await.
        return TaskHandle(
            supervisor=self._supervisor,
            workflow_name=self._workflow_name,
            workflow_run_id=self._workflow_run_id,
            task_id=task_id,
            task_key=task_key
        )

    def spawn_workflow(self, workflow_function, args):
        if hasattr(workflow_function, "__coda_workflow__"):
            workflow_name = workflow_function.__coda_workflow__.workflow_name
        else:
            workflow_name = workflow_function
        workflow_run_id = generate_uuid()

        logging.debug(f"Spawning workflow {workflow_name} in workflow {self._workflow_name}")

        # We store the workflow parameter as a separate process.
        params_id = generate_uuid()
        store_params = self._supervisor.store_params(
            workflow_run_id=workflow_run_id,
            params_id=params_id,
            params=args
        )
        self._supervisor_dispatch(store_params)

        # We spawn the workflow.
        spawn_workflow = self._supervisor.spawn_workflow(
            workflow_name=workflow_name,
            workflow_run_id=workflow_run_id,
            params_id=params_id
        )
        self._supervisor_dispatch(spawn_workflow)
