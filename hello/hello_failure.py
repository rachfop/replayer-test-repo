import asyncio
import uuid
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Dict

from temporalio import activity, workflow
from temporalio.exceptions import ApplicationError


@activity.defn
async def say_hello(name: str) -> str:
    return f"Hello, {name}!"


@dataclass
class SayHelloParams:
    name: str
    should_wait: bool = False
    should_error: bool = False
    should_cause_nondeterminism: bool = False


@workflow.defn
class SayHelloWorkflow:
    def __init__(self) -> None:
        self._waiting = False
        self._finish = False

    @workflow.run
    async def run(self, params: SayHelloParams) -> str:
        result = await workflow.execute_activity(
            say_hello, params.name, schedule_to_close_timeout=timedelta(seconds=60)
        )

        # Wait if requested
        if params.should_wait:
            self._waiting = True
            await workflow.wait_condition(lambda: self._finish)
            self._waiting = False

        # Raise if requested
        if params.should_error:
            raise ApplicationError("Intentional error")

        # Cause non-determinism if requested
        if params.should_cause_nondeterminism:
            if workflow.unsafe.is_replaying():
                await asyncio.sleep(0.1)

        return result

    @workflow.signal
    def finish(self) -> None:
        self._finish = True

    @workflow.query
    def waiting(self) -> bool:
        return self._waiting
