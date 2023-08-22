from coda.task import TaskHandle
from coda.utils import generate_id, hash_cache_key


def coda_workflow(workflow_name):
    def decorator(func):
        func.__workflow_name__ = workflow_name

        return func

    return decorator


class WorkflowHandle:
    def __init__(self, run_id):
        self.run_id = run_id


class WorkflowContext:

    def __init__(self, worker, supervisor, workflow_run_id):
        self.worker = worker
        self.supervisor = supervisor
        self.workflow_run_id = workflow_run_id

    def spawn_task(self, task_function, persistence_key, params):
        task_name = task_function.__name__
        task_id = generate_id()
        task_key = hash_cache_key(
            [self.workflow_run_id, task_name] + list(persistence_key)
        )

        params_id = generate_id()

        self.supervisor.api.spawn_task(
            task_name,
            task_id,
            task_key,
            params_id,
            self.workflow_run_id,
            persistence_key is not None
        )

        return TaskHandle(10, task_key)

    async def await_one(self, task_handle):
        result = await self.worker.register_interest(
            "publish_task_result",
            lambda args: args["task_id"] == task_handle.task_id
        ).get()

        return result
