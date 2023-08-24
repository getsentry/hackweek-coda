import contextvars
from contextvars import ContextVar
from abc import ABC, abstractmethod


class Context(ABC):
    @abstractmethod
    async def spawn_task(self, task_function, args, cache_key=None):
        pass

    @abstractmethod
    async def spawn_workflow(self, workflow_function, args):
        pass

    def __enter__(self):
        self._reset_token = current_context.set(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        current_context.reset(self._reset_token)


current_context = ContextVar('current_context')


def get_current_context():
    return current_context.get()


async def spawn_task(task_function, args, cache_key=None):
    return await get_current_context().spawn_task(task_function, args=args, cache_key=cache_key)


async def spawn_workflow(workflow_function, args):
    return await get_current_context().spawn_workflow(workflow_function, args=args)
