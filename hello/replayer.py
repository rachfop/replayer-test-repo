import asyncio

from hello_activity import GreetingWorkflow
from temporalio.client import Client
from temporalio.worker import Replayer


async def main():
    client = await Client.connect("localhost:7233")

    # Fetch the histories of the workflows to be replayed
    workflows = client.list_workflows('WorkflowId="replayer-workflow-id"')
    histories = workflows.map_histories()
    replayer = Replayer(workflows=[GreetingWorkflow])
    results = await replayer.replay_workflows(histories, raise_on_replay_failure=False)
    print(results)


if __name__ == "__main__":
    asyncio.run(main())
