from coda.task import coda_task


@coda_task()
async def normalize_event(event_data):
    return event_data

@coda_task()
async def symbolicate_event(event_data):
    return event_data

@coda_task()
async def store_event(event_data):
    return event_data
