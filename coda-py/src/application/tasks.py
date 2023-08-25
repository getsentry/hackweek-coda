import time

import coda

class TaskException(Exception):

    def __str__(self):
        return "Exception occurred in a task"


@coda.task()
async def normalize_event(project_id, event_data, **kwargs):
    event_data["project_id"] = project_id
    event_data["normalize_event"] = True
    return event_data


@coda.task(retry_on=[TaskException], max_retries=5)
async def symbolicate_event(project_id, event_data, **kwargs):
    event_data["symbolicate_event"] = True
    if project_id == 2:
        raise TaskException()
    return event_data


@coda.task()
async def store_event(project_id, event_data, **kwargs):
    event_data["store_event"] = True
    event_data["saved_at"] = time.ctime()
    return event_data
