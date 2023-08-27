# Coda Workflow Execution Engine Vision

Coda is a workflow execution engine that aims to draw inspiration from Temporal and enable the execution of workflows with durability and transactional guarantees. The system's goal is to enable developers to define a workflow, which is a series of computational steps guaranteed to be executed in the event of both a workflow failure and a task failure.
For example, suppose you have the following workflow:

```python
def my_workflow():
	orgs = await get_all_orgs()

	tasks = []
	for org in orgs:
		task = send_weekly_email(org)
		tasks.append(task)

	await_all(tasks)

	# Error happening only 75% of the time.
	if random.random() > 0.25:
		1/0

	print("Finished")

```

The workflow is initiated on a worker, and `get\_all\_orgs` is triggered as a task, which will be scheduled on a worker that can support it. This task will execute successfully, and for each organization, the system will spawn other tasks to send the weekly email in `send\_weekly\_email`.
Suppose that a subset of the email sending tasks fails; in that case, the system will attempt to retry them up to a certain number of times. Afterward, it will either make the `await\_all` operation succeed or fail. If the system encounters a failure, such as a division by zero (1/0), it will retry the entire workflow. However, it will recognize that the organizations and tasks for sending emails have already been computed, and thus it will not retry them. Instead, it will replay all the events up to the last checkpoint and continue from there as usual.
To achieve such behavior, several components are required, and many interactions will occur under the hood. In our first iteration, we identified the following components of our architecture:
- **Clients** → They will be responsible for sending commands to one of the supervisors in order to trigger the execution of a workflow.
- **Workers** → They will be responsible for executing tasks and workflows by declaring what they can execute.
- **Supervisors** → They will be responsible for communicating with Flow and orchestrating groups of workers on the same machine. Supervisors will also communicate with Flow regarding all the merged tasks/workflows they can accept, and they might perform batching and caching for faster performance. For instance, when many workers spawned by the supervisor require access to the same task result, it will be much faster if the system caches it.
- **Flow** → It will be responsible for holding queues and workflow/task states in persistent data storage. At any point in time, Flow should know what is happening in the system, and any action taken by the system should first be confirmed by Flow since it will be durably persisted.

# Components
## Client
The client is a component that will request workflow executions. We would like to have an etherogenous set of clients, since we want to give the ability to the developers to start workflows from multiple places like our SDKs but also CLI.
## Worker
The worker will be responsible for executing the actual workflow/task code and communicate the results/failures upstream. It will also contain all the logic for the suspension behavior of workflows.
## Supervisor
The supervisor is a component that will perform the following tasks:
1. **Spawning and shutting down workers:** The supervisor will be responsible for managing the lifecycle of the workers.
2. **Forwarding messages to and from workers to and from Flow:** The supervisor will act as a proxy between Flow and workers. Some protocol messages will be exclusively exchanged between Flow and the supervisor, potentially involving handshakes and specific commands.
3. **Caching frequently accessed task results:** There may be a need to access a task result multiple times within a worker. For this reason, caching in the supervisor can help reduce the round trip for obtaining results. This becomes especially important if the system has many workers under the same supervisors, and these workers frequently spawn the same task (which could happen more frequently if developers misuse Coda).
4. **Keeping Flow updated regarding the number of workflows/tasks supported by the supervisor:** The supervisor will maintain a set of workflows and tasks that it can assign to its workers. Since workers can spawn or terminate, they may support a varying number of workflows/tasks. This state change must be communicated upstream to ensure consistent management of the ephemeral queues.
5. **Tracking the state of the workers:** The supervisor must monitor the state of workers by keeping track of the workflows/tasks they are executing. This enables centralized cancellation within the supervisor, allowing workers to remain as stateless as possible. Cancellation of active workflows is crucial, as developers may decide to terminate long-running workflows.

## Flow
Flow is a component that will perform the following tasks:
1. **Persisting queues for workflow and task executions:** The idea is to have a queue for each workflow and task type so that all scheduled executions persist on the same queue. To ensure transactional behavior, the system will maintain queues and state persistence within the same database shard.
2. **Persisting the workflow and task state:** The goal is to store all the information that the system can use to rapidly reconstruct the computational state after a failure.
3. **Creating ephemeral queues for each specific supervisor, with a policy to populate these queues with messages that need to be delivered to the supervisor:** The concept here is that a supervisor will express interest in a set of tasks and workflows, and these interests will be used to create an ephemeral queue from which the supervisor can consume messages. This queue can be filled using several heuristics, such as round-robin or work-stealing.
4. **Managing retry policies for each workflow and task:** Flow will have to keep track of explicit and byzantine failures happening in supervisors/workers. The types of failures that can occur include:
    - A Supervisor/Worker communicates a failure due to a caught exception.
    - A Supervisor/Worker experiences a byzantine failure, which we can detect only by setting upper bounds on the execution time or constant health checks.

    Based on whether a supervisor or worker is experiencing a failure, the system will have to act accordingly. For example, if a worker fails, the system will need to know which workflows/tasks it was running to reschedule execution. On the other hand, if the entire supervisor fails, Flow should detect it and clear the ephemeral queue. Then, it should determine what to do with the pending workflows that Flow knows must have been executed by the supervisor's workers.
5. **Maintaining queryable workflow states that can be accessed during execution to obtain specific values required for computation:** Such state can also be shared between workflows if the need arises. It will act as a sort of broadcast variable that can be efficiently cached in each supervisor.
6. **Handling durable timers and cron jobs:** This includes executing workflows at specific times and ensuring that, in case of failures, no two identical workflows coexist. For example, if a workflow is scheduled every minute, and the first one fails, the second one should start after 1 minute, and the first one should not be retried. In such cases, better semantics should be defined.

# **Open Questions**
1. How will code versioning work? For example, what happens if you deploy code changes while some workers are still handling the old workflow?
2. How will failures be detected and handled?
3. Should Coda support workflow results?
4. Should Coda support cross-workflow shared state that can be queried at execution time?
