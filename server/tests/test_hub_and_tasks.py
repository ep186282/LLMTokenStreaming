from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from server.hub import WakeupHub
from server.tasks import GenerationTaskRegistry


@pytest.mark.asyncio
async def test_hub_wakes_every_registered_reader() -> None:
    generation_id = uuid4()
    hub = WakeupHub()
    first = await hub.register(generation_id)
    second = await hub.register(generation_id)

    await hub.publish(generation_id)

    assert first.is_set()
    assert second.is_set()
    await hub.unregister(generation_id, first)
    assert await hub.subscriber_count(generation_id) == 1
    await hub.unregister(generation_id, second)
    assert await hub.subscriber_count(generation_id) == 0


@pytest.mark.asyncio
async def test_registry_starts_one_task_and_signals_cancellation() -> None:
    generation_id = uuid4()
    registry = GenerationTaskRegistry()
    started = 0
    observed_cancel = asyncio.Event()

    async def runner(cancel_requested: asyncio.Event) -> None:
        nonlocal started
        started += 1
        await cancel_requested.wait()
        observed_cancel.set()

    results = await asyncio.gather(
        *[
            registry.ensure_started(generation_id, runner)
            for _ in range(20)
        ]
    )

    assert sum(results) == 1
    assert started == 1
    assert await registry.signal_cancel(generation_id)
    await asyncio.wait_for(observed_cancel.wait(), timeout=1)
    await registry.shutdown()
