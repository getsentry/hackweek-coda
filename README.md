<div align="center">
  <img src="https://github.com/mitsuhiko/coda/blob/main/assets/logo.png?raw=true" alt="" width=320>
  <p><strong>Coda:</strong> capisci le code?</p>
</div>

Celery just less terrible.

# What's Coda?

Coda is a hackweek project to try some alternative ideas to how queue systems can work.
We use a lot of Celery at Sentry and while Celery is not a bad project, it's not a great
abstraction for many of the things we want to build.

At Sentry some tasks behave a bit like workflows that depend on more than one task.  They
use redis as auxiliary storage to pass data around while different tasks are executing.
The end result is a pretty big mess that can be hard to understand.

We quite appreciate the design of temporal but unfortunately it's design is quite heavy
weight for many of our operations.  Coda tries to explore of the ideas can also work in
a more simplified world.

# Example

This workflow starts two tasks.  When the workflow reruns because for instance it
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

# Design Ideas

Coda consts of a **supervisor** which spawns workers.  For now there is only a Python client.
The workers pass all their communication via the supervisor.  They connect neither to a queue
nor a state management system.  All that data is handled via the supervisor.

The supervisor's job is to talk to the actually workflow engine.  This has not been implemented
yet.  All state is instead held in memory directly in the supervisor.  All state is stored
with the workflow.
