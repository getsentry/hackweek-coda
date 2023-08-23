import logging


def coda_task(task_name):
    def decorator(func):
        func.__task_name__ = task_name

        return func

    return decorator


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
        logging.debug(f"Task {self._task_id} finished with result {result}")
        return result
