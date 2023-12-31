from subprocess import call

import coda
from .tasks import normalize_event, symbolicate_event, store_event
from .utils import needs_symbolication


@coda.workflow()
async def process_event(project_id, event_data, **kwargs):
    event_data = await coda.context.spawn_task(
        normalize_event,
        args={"project_id": project_id, "event_data": event_data},
        cache_key=[event_data["event_id"]]
    )

    if needs_symbolication(event_data):
        event_data = await coda.context.spawn_task(
            symbolicate_event,
            args={"project_id": project_id, "event_data": event_data},
            cache_key=[event_data["event_id"]]
        )

    result = await coda.context.spawn_task(
        store_event,
        args={"project_id": project_id, "event_data": event_data},
        cache_key=[event_data["event_id"]]
    )

    call([
        "say",
        "-vAlice",
        f"Il lavoratore ha finito alle {result['saved_at']}"
    ])
