def coda_task(task_name):
    def decorator(func):
        func.__task_name__ = task_name

        return func

    return decorator


class TaskHandle:
    def __init__(self, task_id, task_key):
        self.task_id = task_id
        self.task_key = task_key
