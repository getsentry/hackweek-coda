import coda


@coda.task()
async def normalize_event(event_data):
    return event_data

@coda.task()
async def symbolicate_event(event_data):
    return event_data

@coda.task()
async def store_event(event_data):
    return event_data
