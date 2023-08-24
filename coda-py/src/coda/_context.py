from abc import ABC, abstractmethod
from contextvars import ContextVar


class Context(ABC):
    @abstractmethod
    def spawn_task(self, task_function, args, cache_key=None):
        pass

    @abstractmethod
    def spawn_workflow(self, workflow_function, args):
        pass

    def __enter__(self):
        self._reset_token = _current_context.set(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _current_context.reset(self._reset_token)


_current_context = ContextVar('current_context')


class _CurrentContext(Context):

    def spawn_task(self, task_function, args, cache_key=None):
        return _current_context.get().spawn_task(task_function, args=args, cache_key=cache_key)

    def spawn_workflow(self, workflow_function, args):
        return _current_context.get().spawn_workflow(workflow_function, args=args)


context = _CurrentContext()
