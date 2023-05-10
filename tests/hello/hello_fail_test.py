import uuid
from typing import Dict

import pytest
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Replayer, Worker

from hello.hello_failure import SayHelloParams, SayHelloWorkflow, say_hello


async def test_replayer_workflow_complete(client: Client) -> None:
    task_queue_name = str(uuid.uuid4())

    async with Worker(
        client,
        task_queue=task_queue_name,
        workflows=[SayHelloWorkflow],
        activities=[say_hello],
    ):
        async with new_say_hello_worker(client) as worker:
            handle = await client.start_workflow(
                SayHelloWorkflow.run,
                SayHelloParams(name="Temporal"),
                id=f"workflow-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            assert "Hello, Temporal!" == await handle.result()

        await Replayer(workflows=[SayHelloWorkflow]).replay_workflow(
            await handle.fetch_history()
        )


async def test_replayer_workflow_nondeterministic(client: Client) -> None:
    # Run workflow to completion
    async with new_say_hello_worker(client) as worker:
        handle = await client.start_workflow(
            SayHelloWorkflow.run,
            SayHelloParams(name="Temporal", should_cause_nondeterminism=True),
            id=f"workflow-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )
        await handle.result()
    with pytest.raises(workflow.NondeterminismError):
        await Replayer(workflows=[SayHelloWorkflow]).replay_workflow(
            await handle.fetch_history()
        )


@workflow.defn
class SayHelloWorkflowDifferent:
    @workflow.run
    async def run(self) -> None:
        pass


def new_say_hello_worker(client: Client) -> Worker:
    return Worker(
        client,
        task_queue=str(uuid.uuid4()),
        workflows=[SayHelloWorkflow],
        activities=[say_hello],
    )
