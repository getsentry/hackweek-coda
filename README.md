<div align="center">
  <img src="https://github.com/mitsuhiko/coda/blob/main/assets/logo.png?raw=true" alt="" width=320>
</div>

Meet **Coda**, a hackweek project on a mission to revolutionize queue systems and simplify complex task workflows at [Sentry](https://sentry.io).

# What's Coda?

Coda is a hackweek project aimed at exploring alternative ideas for how queue systems can function. We extensively employ
Celery at Sentry, and although Celery is not a bad project, it does not offer an ideal abstraction for many of the
functionalities we intend to develop.

Within Sentry, some async jobs exhibit workflow-like behavior, relying on multiple tasks for their execution.
These tasks mainly rely on Redis as auxiliary storage to exchange data during the execution, resulting in many
rather complex and challenging implementations, which can differ significantly.

We quite appreciate the design of [Temporal](https://temporal.io/), but unfortunately it is quite heavyweight for many of our
operations. Coda is our attempt at building a workflow engine that mixes simplicity with some of Temporal's ideas.

# Example

This workflow starts two tasks. When the workflow reruns because for instance it
failed, it will not invoke tasks which have already yielded a result (as keyed by
`cache_key`).

```python
import coda

from .tasks import normalize_event, store_event

@coda.workflow()
async def process_event(project_id, event_data, **kwargs):
    event_data = await coda.context.spawn_task(
        normalize_event,
        args={"project_id": project_id, "event_data": event_data},
        cache_key=[event_data["event_id"]]
    )
    result = await coda.context.spawn_task(
        store_event,
        args={"project_id": project_id, "event_data": event_data},
        cache_key=[event_data["event_id"]]
    )
    print(f"Workflow finished with result {result}")
```

# High-Level Design

Coda consists of a **supervisor** responsible for spawning workers. Currently, there is only a Python client available.
The workers relay all their communication through the supervisor, connecting to neither a queue nor a state management
system. All data is managed by the supervisor.

The supervisor's primary responsibility is to communicate with the actual workflow engine, which has not been
implemented yet. Consequently, all state data is stored directly in memory within the supervisor. This state is
associated with the workflow.
