from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from server.config import Settings
from server.database import Database, GenerationProgress, TerminalStatus
from server.generator import GenerationRunner
from server.hub import WakeupHub
from server.providers import Provider, ProviderEvent
from server.tasks import GenerationTaskRegistry


class FakeGeneratorDatabase:
    def __init__(self) -> None:
        self.chunks: list[tuple[int, dict[str, Any]]] = []
        self.batches: list[list[int]] = []
        self.finished: list[tuple[TerminalStatus, int]] = []
        self.cancel_requested = False

    async def get_progress(
        self,
        generation_id: UUID,
    ) -> GenerationProgress:
        del generation_id
        return GenerationProgress(
            status="running",
            current_seq=self.chunks[-1][0] if self.chunks else 0,
            cancel_requested=self.cancel_requested,
        )

    async def insert_chunks(
        self,
        generation_id: UUID,
        chunks: list[tuple[int, dict[str, Any]]],
    ) -> None:
        del generation_id
        expected = self.chunks[-1][0] + 1 if self.chunks else 1
        assert [sequence for sequence, _ in chunks] == list(
            range(expected, expected + len(chunks))
        )
        self.chunks.extend(chunks)
        self.batches.append([sequence for sequence, _ in chunks])

    async def finish_generation(
        self,
        generation_id: UUID,
        *,
        desired_status: TerminalStatus,
        final_seq: int,
        finish_reason: str | None = None,
        usage: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> TerminalStatus:
        del generation_id, finish_reason, usage, error
        actual_status: TerminalStatus = (
            "cancelled" if self.cancel_requested else desired_status
        )
        self.finished.append((actual_status, final_seq))
        return actual_status


class AmbiguousCommitDatabase(FakeGeneratorDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.insert_calls = 0

    async def insert_chunks(
        self,
        generation_id: UUID,
        chunks: list[tuple[int, dict[str, Any]]],
    ) -> None:
        self.insert_calls += 1
        if self.insert_calls == 1:
            await super().insert_chunks(generation_id, chunks)
            raise ConnectionError("commit acknowledgement was lost")

        assert chunks == self.chunks


class RecoveringTerminalDatabase(FakeGeneratorDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.allow_terminal = asyncio.Event()
        self.reconciliation_started = asyncio.Event()
        self.terminal_persisted = asyncio.Event()
        self.finish_attempts = 0

    async def finish_generation(
        self,
        generation_id: UUID,
        *,
        desired_status: TerminalStatus,
        final_seq: int,
        finish_reason: str | None = None,
        usage: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> TerminalStatus:
        self.finish_attempts += 1
        if self.finish_attempts >= 3:
            self.reconciliation_started.set()
        if not self.allow_terminal.is_set():
            raise ConnectionError("database remains unavailable")

        status = await super().finish_generation(
            generation_id,
            desired_status=desired_status,
            final_seq=final_seq,
            finish_reason=finish_reason,
            usage=usage,
            error=error,
        )
        self.terminal_persisted.set()
        return status


class FastProvider:
    def __init__(self) -> None:
        self.starts = 0

    async def stream(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
    ):
        del model, messages
        self.starts += 1
        for number in range(5):
            yield {
                "type": "text.delta",
                "text": str(number),
            }
        yield {
            "type": "provider.completed",
            "finish_reason": "stop",
            "usage": {"output_tokens": 5},
        }


class SlowFlushDatabase(FakeGeneratorDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.second_delta_pulled = asyncio.Event()

    async def insert_chunks(
        self,
        generation_id: UUID,
        chunks: list[tuple[int, dict[str, Any]]],
    ) -> None:
        if not self.second_delta_pulled.is_set():
            await asyncio.wait_for(self.second_delta_pulled.wait(), timeout=1)
        await super().insert_chunks(generation_id, chunks)


class PipelinedProvider:
    def __init__(self, second_delta_pulled: asyncio.Event) -> None:
        self.second_delta_pulled = second_delta_pulled

    async def stream(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
    ):
        del model, messages
        yield {"type": "text.delta", "text": "one"}
        self.second_delta_pulled.set()
        yield {"type": "text.delta", "text": "two"}
        yield {"type": "provider.completed", "finish_reason": "stop"}


class PausingProvider:
    def __init__(self) -> None:
        self.paused = asyncio.Event()
        self.release = asyncio.Event()

    async def stream(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
    ):
        del model, messages
        yield {"type": "text.delta", "text": "first"}
        self.paused.set()
        await self.release.wait()
        yield {"type": "text.delta", "text": "second"}


def settings(*, flush_deltas: int, flush_ms: float) -> Settings:
    return Settings(
        database_url="postgres://unused",
        openrouter_api_key="test-key",
        model="openai/gpt-4o-mini",
        openrouter_base_url="https://openrouter.ai/api/v1",
        flush_max_deltas=flush_deltas,
        flush_max_ms=flush_ms,
        snapshot_threshold=50,
        heartbeat_s=15,
        client_origin="http://localhost:5173",
    )


def runner(
    database: FakeGeneratorDatabase,
    provider: object,
    *,
    flush_deltas: int,
    flush_ms: float,
) -> GenerationRunner:
    return GenerationRunner(
        database=cast(Database, database),
        provider=cast(Provider, provider),
        hub=WakeupHub(),
        settings=settings(
            flush_deltas=flush_deltas,
            flush_ms=flush_ms,
        ),
    )


@pytest.mark.asyncio
async def test_generator_assigns_dense_sequences_and_batches_flushes() -> None:
    database = FakeGeneratorDatabase()
    generation = runner(
        database,
        FastProvider(),
        flush_deltas=2,
        flush_ms=10_000,
    )

    await generation.run(
        uuid4(),
        model="openai/gpt-4o-mini",
        messages=[{"role": "user", "content": "hello"}],
        cancel_requested=asyncio.Event(),
    )

    assert database.batches == [[1, 2], [3, 4], [5]]
    assert database.finished == [("completed", 5)]


@pytest.mark.asyncio
async def test_generator_pulls_next_provider_event_during_flush() -> None:
    database = SlowFlushDatabase()
    generation = runner(
        database,
        PipelinedProvider(database.second_delta_pulled),
        flush_deltas=1,
        flush_ms=10_000,
    )

    await asyncio.wait_for(
        generation.run(
            uuid4(),
            model="openai/gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
            cancel_requested=asyncio.Event(),
        ),
        timeout=2,
    )

    assert database.batches == [[1], [2]]
    assert database.finished == [("completed", 2)]


@pytest.mark.asyncio
async def test_generator_observes_cancel_during_provider_pause() -> None:
    database = FakeGeneratorDatabase()
    provider = PausingProvider()
    generation = runner(
        database,
        provider,
        flush_deltas=8,
        flush_ms=10,
    )
    cancel_requested = asyncio.Event()
    task = asyncio.create_task(
        generation.run(
            uuid4(),
            model="openai/gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
            cancel_requested=cancel_requested,
        )
    )
    await asyncio.wait_for(provider.paused.wait(), timeout=1)
    await asyncio.sleep(0.03)

    cancel_requested.set()
    await asyncio.wait_for(task, timeout=1)

    assert [sequence for sequence, _ in database.chunks] == [1]
    assert database.finished == [("cancelled", 1)]


@pytest.mark.asyncio
async def test_shutdown_cancellation_flushes_without_terminal_state() -> None:
    database = FakeGeneratorDatabase()
    provider = PausingProvider()
    generation = runner(
        database,
        provider,
        flush_deltas=8,
        flush_ms=10_000,
    )
    task = asyncio.create_task(
        generation.run(
            uuid4(),
            model="openai/gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
            cancel_requested=asyncio.Event(),
        )
    )
    await asyncio.wait_for(provider.paused.wait(), timeout=1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert [sequence for sequence, _ in database.chunks] == [1]
    assert database.finished == []


@pytest.mark.asyncio
async def test_ambiguous_commit_keeps_the_persisted_final_sequence() -> None:
    database = AmbiguousCommitDatabase()
    generation = runner(
        database,
        FastProvider(),
        flush_deltas=2,
        flush_ms=10_000,
    )

    await generation.run(
        uuid4(),
        model="openai/gpt-4o-mini",
        messages=[{"role": "user", "content": "hello"}],
        cancel_requested=asyncio.Event(),
    )

    assert [sequence for sequence, _ in database.chunks] == [1, 2]
    assert database.finished == [("failed", 2)]


@pytest.mark.asyncio
async def test_crashed_writer_keeps_ownership_until_terminal_is_persisted() -> None:
    database = RecoveringTerminalDatabase()
    provider = FastProvider()
    generation = runner(
        database,
        provider,
        flush_deltas=2,
        flush_ms=10_000,
    )
    registry = GenerationTaskRegistry()
    generation_id = uuid4()

    def start(cancel_requested: asyncio.Event) -> Awaitable[None]:
        return generation.run(
            generation_id,
            model="openai/gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
            cancel_requested=cancel_requested,
        )

    assert await registry.ensure_started(generation_id, start)
    await asyncio.wait_for(database.reconciliation_started.wait(), timeout=1)

    assert await registry.active_count() == 1
    assert not await registry.ensure_started(generation_id, start)
    assert provider.starts == 1

    database.allow_terminal.set()
    await asyncio.wait_for(database.terminal_persisted.wait(), timeout=1)

    async def wait_until_idle() -> None:
        while await registry.active_count():
            await asyncio.sleep(0)

    await asyncio.wait_for(wait_until_idle(), timeout=1)
    assert database.finished == [("failed", 5)]
    assert provider.starts == 1
