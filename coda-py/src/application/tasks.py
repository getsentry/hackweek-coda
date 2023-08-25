import time
import random

import coda

class TaskException(Exception):
    pass


@coda.task()
async def normalize_event(project_id, event_data, **kwargs):
    event_data["project_id"] = project_id
    event_data["normalize_event"] = True
    return event_data


@coda.task(retry_on=[TaskException], max_retries=10)
async def symbolicate_event(event_data, **kwargs):
    event_data["symbolicate_event"] = True
    if random.random() > 0.25:
        raise TaskException("symbolication failed because flaky")
    return event_data


@coda.task()
async def store_event(event_data, **kwargs):
    event_data["store_event"] = True
    event_data["saved_at"] = time.ctime()
    return event_data
