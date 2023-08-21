def coda_task(task_name):
    def decorator(func):
        func.__task_name__ = task_name

        return func

    return decorator
