from core.coda_utils import generate_id, hash_cache_key


def coda_workflow(workflow_name):
    def decorator(cls):
        def set_context(self, context):
            self.context = context

        cls.__workflow_name__ = workflow_name
        cls.set_context = set_context

        return cls

    return decorator


class WorkflowContext:

    def __init__(self, workflow_run_id, supervisor):
        self.workflow_run_id = workflow_run_id
        self.supervisor = supervisor

    def spawn_task(self, task_function, persistence_key, params):
        task_name = task_function.__name__
        task_id = generate_id()
        task_key = hash_cache_key(
            [self.workflow_run_id, task_name] + list(persistence_key)
        )

        # TODO: call spawn task.

        print(f"Executing task {task_name}")