import coda


@coda.task()
async def normalize_event(project_id, event_data):
    return event_data


@coda.task()
async def symbolicate_event(project_id, event_data):
    return event_data


@coda.task()
async def store_event(project_id, event_data):
    print("EXECUTING STORE")
    return event_data
