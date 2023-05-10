import uuid

from temporalio import activity
from temporalio.client import Client
from temporalio.worker import Replayer, Worker

from hello.hello_activity import (
    ComposeGreetingInput,
    GreetingWorkflow,
    compose_greeting,
)


async def test_execute_workflow(client: Client):
    task_queue_name = str(uuid.uuid4())

    async with Worker(
        client,
        task_queue=task_queue_name,
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
    ):
        assert "Hello, World!" == await client.execute_workflow(
            GreetingWorkflow.run,
            "World",
            id=str(uuid.uuid4()),
            task_queue=task_queue_name,
        )


@activity.defn(name="compose_greeting")
async def compose_greeting_mocked(input: ComposeGreetingInput) -> str:
    return f"{input.greeting}, {input.name} from mocked activity!"


async def test_mock_activity(client: Client):
    task_queue_name = str(uuid.uuid4())
    async with Worker(
        client,
        task_queue=task_queue_name,
        workflows=[GreetingWorkflow],
        activities=[compose_greeting_mocked],
    ):
        assert "Hello, World from mocked activity!" == await client.execute_workflow(
            GreetingWorkflow.run,
            "World",
            id=str(uuid.uuid4()),
            task_queue=task_queue_name,
        )


async def test_replay_workflow(client: Client):
    task_queue_name = str(uuid.uuid4())

    async with Worker(
        client,
        task_queue=task_queue_name,
        workflows=[GreetingWorkflow],
        activities=[compose_greeting],
    ):
        await client.execute_workflow(
            GreetingWorkflow.run,
            "World",
            id=str(uuid.uuid4()),
            task_queue=task_queue_name,
        )

        workflows = client.list_workflows(f'WorkflowId="{id}"')
        histories = workflows.map_histories()
        replayer = Replayer(workflows=[GreetingWorkflow])
        results = await replayer.replay_workflows(
            histories, raise_on_replay_failure=False
        )
        print(results)
        assert "WorkflowReplayResults(replay_failures={})" == str(results)
