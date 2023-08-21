import q
import uuid
import json
import hashlib
from weakref import ref as weakref


def iter_over_all_orgs():
    return iter(())


class Workflow:
    def __init__(self, ctx):
        self._ctx = weakref(ctx)
        self.run_id = uuid.uuid4()

    @property
    def ctx(self):
        rv = self._ctx()
        if rv is None:
            raise RuntimeError('Context went away')
        return rv

    def set_retry_policy(self, max_retries):
        return self.ctx.master.send_request({
            "cmd": "set_workflow_retry_policy",
            "args": {
                "workflow_run_id": self.run_id,
                "max_retries": max_retries,
            }
        })


def hash_cache_key(items):
    h = hashlib.md5()
    for item in items:
        h.update(str(item).encode("utf-8"))
    return h.hexdigest()


class WorkflowHandle:
    def __init__(self, run_id):
        self.run_id = run_id


class TaskHandle:
    def __init__(self, task_id, task_key):
        self.task_id = task_id
        self.task_key = task_key


class Context:
    def __init__(self):
        self.workflow = Workflow()

    def new_uuid(self):
        # TODO: deterministic
        return uuid.uuid4()

    def start_workflow(self, workflow_name, params):
        run_id = self.new_uuid()
        params_id = self.new_uuid()
        
        self.master.send_request({
            "cmd": "store_params",
            "args": {
                "workflow_run_id": self.workflow.run_id,
                "params_id": params_id,
                "params": params,
            },
        })

        self.master.send_request({
            "cmd": "start_workflow",
            "args": {
                "workflow_name": workflow_name,
                "workflow_run_id": run_id,
                "params_id": params_id,
            },
        })
        return WorkflowHandle(run_id)

    def spawn_cached(self, task_name, cache_key, params):
        task_key = hash_cache_key(
            [self.workflow.run_id, task_name] + list(cache_key)
        )

        # Check if we already ran
        task_id = self.master.send_request({
            "cmd": "get_finished_task_id",
            "args": {
                # We don't need the task_id here, since it's just used
                # to disambiguate the at least once semantics of execution.
                "task_key": task_key,
                "workflow": self.workflow.run_id,
            }
        })
        if task_id:
            return TaskHandle(task_id, task_key)

        params_id = self.new_uuid()
        task_id = self.new_uuid()
        self.master.send_request({
            "cmd": "store_params",
            "args": {
                "workflow_run_id": self.workflow.run_id,
                "params_id": params_id,
                "params": params,
            },
        })

        self.master.send_request({
            "cmd": "spawn_task",
            "args": {
                "task_name": task_name,
                "task_id": task_id,
                "task_key": task_key,
                "params_id": params_id,
                "workflow_run_id": self.workflow.run_id,
                "persist_result": True,   # cached means persist result
            },
        })
        
        return TaskHandle(task_id, task_key)

    async def await_one(self, task_handle):
        # Either we wait for a response or we need to somehow subscribe
        # on the manager to get new events.
        return await self.master.send_request({
            "cmd": "get_task_result",
            "args": {
                "task_id": task_handle.task_id,
                "taks_key": task_handle.task_key
            }
        })


@q.workflow("boost_low_volume_projects")
async def boost_low_volume_projects(ctx):
    tasks = []

    for orgs in iter_over_all_orgs():
        for project in orgs.projects:
            tasks.append(ctx.spawn_cached(
                "boost_low_volume_projects_of_org",
                cache_key=[org_id, project_id],
                params=dict(
                    org_id=org_id,
                    project_id=project_id,
                    project_data=project.to_dict(),
                )
            ))

    results = await ctx.await_all(tasks)



@q.workflow("process_and_store_event")
async def process_and_store_event(ctx, project_id, event_data):
    ctx.workflow.set_retry_policy(
        max_retries=5
    )

    task_handle = ctx.spawn_cached("normalize_event", [event_data["event_id"]], params=dict(
        event_data=event_data,
        project_id=project_id,
    ))

    new_event_data = await ctx.await_one(task_handle)
    if new_event_data is not None:
        event_data = new_event_data

    if needs_symbolication(event_data):
        task_handle = ctx.spawn_cached("symbolicate_event", [event_data["event_id"]], params=dict(
            event_data=event_data,
            project_id=project_id,
        ))

        new_event_data = await ctx.await_one(task_handle)
        if new_event_data:
            event_data = new_event_data

    task_handle = ctx.spawn_cached("store_event", [event_data["event_id"]], params=dict(
        event_data=event_data,
        project_id=project_id,
    ))
    await ctx.await_one(task_handle)
    return True


@q.task("normalize_event")
def normalize_event(ctx, event_data):
    new_event_data, changed = normalize_event_data(event_data)
    if changed:
        return new_event_data


@q.task("fetch_url")
def fetch_url(ctx, url):
    task_key = ctx.current_task.task_key
    fetcher.submit_fetch_request(url, task_key=task_key)

    # when fetch is done it calls ctx.submit_result(task_key, result)
    return q.RESULT_PENDING


def kafka_consumer(ctx):
    for batch in client:
        workflows = []
        for message in batch:
            workflows.append(ctx.spawn_workflow("process_and_store_event", {
                "project_id": message["project_id"],
                "event_data": message["event_data"],
            }))
        ctx.await_all(workflows)
        batch.commit()
