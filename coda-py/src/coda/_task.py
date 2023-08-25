import logging
from weakref import ref as weakref

from coda._utils import get_object_name


def task(task_name=None, retry_on=None, max_retries=0):
    def decorator(func):
        inner_task = Task(
            task_name=task_name or get_object_name(func),
            retry_on=retry_on or [],
            max_retries=max_retries or 0,
            func=func
        )
        func.__coda_task__ = inner_task

        return func

    return decorator


class Task:
    def __init__(self, task_name, retry_on, max_retries, func):
        self.task_name = task_name
        self._retry_on = retry_on
        self.max_retries = max_retries
        self._func = weakref(func)

    def retriable_for(self, exc):
        for retry_on_exc in self._retry_on:
            if isinstance(exc, retry_on_exc):
                return True

        return False

    def __call__(self, *args, **kwargs):
        return self._func()(*args, **kwargs)


class TaskHandle:
    def __init__(self, supervisor, workflow_name, workflow_run_id, task_id, task_key):
        self._supervisor = supervisor
        self._workflow_name = workflow_name
        self._workflow_run_id = workflow_run_id
        self._task_id = task_id
        self._task_key = task_key

    def __await__(self):
        logging.debug(f"Waiting for task {self._task_id} in workflow {self._workflow_name}")
        result = self._supervisor.get_task_result(self._workflow_run_id, self._task_key).__await__()
        return result
