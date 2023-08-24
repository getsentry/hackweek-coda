import coda


@coda.task()
async def normalize_event(project_id, event_data):
    event_data["normalize_event"] = True
    return event_data


@coda.task()
async def symbolicate_event(project_id, event_data):
    event_data["symbolicate_event"] = True
    return event_data


@coda.task()
async def store_event(project_id, event_data):
    event_data["store_event"] = True
    return event_data
