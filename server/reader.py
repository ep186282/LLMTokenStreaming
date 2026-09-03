from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID

from server.database import Database, ReadBatch, ReadState, StoredChunk
from server.hub import WakeupHub


logger = logging.getLogger("uvicorn.error")


class ReaderGenerationNotFound(Exception):
    pass


class CursorAhead(Exception):
    def __init__(self, current_seq: int) -> None:
        super().__init__(f"cursor exceeds current sequence {current_seq}")
        self.current_seq = current_seq


def _sse(
    *,
    event: str,
    data: dict[str, Any],
    event_id: int | None = None,
) -> str:
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    lines.append(
        "data: "
        + json.dumps(
            data,
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    return "\n".join(lines) + "\n\n"


def _terminal_sse(state: ReadState) -> str:
    final_seq = state.final_seq
    if final_seq is None:
        raise RuntimeError("terminal generation has no final sequence")

    data: dict[str, Any] = {"final_seq": final_seq}
    if state.status == "completed":
        if state.finish_reason is not None:
            data["finish_reason"] = state.finish_reason
        if state.usage is not None:
            data["usage"] = state.usage
        event = "done"
    elif state.status == "cancelled":
        if state.finish_reason is not None:
            data["finish_reason"] = state.finish_reason
        event = "cancelled"
    elif state.status == "failed":
        if state.error is not None:
            data["error"] = state.error
        event = "failed"
    elif state.status == "interrupted":
        data["reason"] = state.error or "server_restart"
        event = "interrupted"
    else:
        raise RuntimeError("running generation has no terminal event")
    return _sse(event=event, data=data, event_id=final_seq)


class ReaderSession:
    def __init__(
        self,
        *,
        database: Database,
        hub: WakeupHub,
        generation_id: UUID,
        cursor: int,
        snapshot_threshold: int,
        heartbeat_s: float,
        wakeup: asyncio.Event,
    ) -> None:
        self._database = database
        self._hub = hub
        self._generation_id = generation_id
        self._cursor = cursor
        self._snapshot_threshold = snapshot_threshold
        self._heartbeat_s = heartbeat_s
        self._wakeup = wakeup
        self._initial_batch: ReadBatch | None = None
        self._opened_at = time.perf_counter()
        self._closed = False

    @classmethod
    async def open(
        cls,
        *,
        database: Database,
        hub: WakeupHub,
        generation_id: UUID,
        cursor: int,
        snapshot_threshold: int,
        heartbeat_s: float,
    ) -> ReaderSession:
        opened_at = time.perf_counter()
        # Register before the first read so a commit between catch-up and
        # waiting cannot be missed.
        wakeup = await hub.register(generation_id)
        session = cls(
            database=database,
            hub=hub,
            generation_id=generation_id,
            cursor=cursor,
            snapshot_threshold=snapshot_threshold,
            heartbeat_s=heartbeat_s,
            wakeup=wakeup,
        )
        session._opened_at = opened_at
        try:
            session._wakeup.clear()
            batch = await database.read_batch(generation_id, cursor)
            if batch is None:
                raise ReaderGenerationNotFound
            if cursor > batch.state.current_seq:
                raise CursorAhead(batch.state.current_seq)
            session._initial_batch = batch
        except BaseException:
            await session.close(client_abort=False)
            raise

        if batch.chunks:
            initial_range = f"{batch.chunks[0].seq}-{batch.chunks[-1].seq}"
        else:
            initial_range = "empty"
        elapsed_ms = (time.perf_counter() - opened_at) * 1000
        logger.info(
            "reader attach gen=%s cursor=%d initial=%s elapsed_ms=%.1f",
            str(generation_id)[:8],
            cursor,
            initial_range,
            elapsed_ms,
        )
        return session

    async def close(self, *, client_abort: bool) -> None:
        if self._closed:
            return
        self._closed = True
        await self._hub.unregister(self._generation_id, self._wakeup)
        elapsed_ms = (time.perf_counter() - self._opened_at) * 1000
        logger.info(
            "reader detach gen=%s cursor=%d client_abort=%s elapsed_ms=%.1f",
            str(self._generation_id)[:8],
            self._cursor,
            str(client_abort).lower(),
            elapsed_ms,
        )

    def _render_chunks(self, chunks: list[StoredChunk]) -> list[str]:
        if len(chunks) > self._snapshot_threshold:
            self._cursor = chunks[-1].seq
            return [
                _sse(
                    event="snapshot",
                    event_id=self._cursor,
                    data={
                        "events": [
                            {"seq": chunk.seq, "event": chunk.event}
                            for chunk in chunks
                        ]
                    },
                )
            ]

        rendered: list[str] = []
        for chunk in chunks:
            self._cursor = chunk.seq
            rendered.append(
                _sse(
                    event="chunk",
                    event_id=chunk.seq,
                    data=chunk.event,
                )
            )
        return rendered

    async def events(self) -> AsyncIterator[str]:
        terminal_sent = False
        client_abort = False
        batch = self._initial_batch
        self._initial_batch = None

        try:
            while True:
                if batch is None:
                    # Wakeups are hints. The cursor-based query remains authoritative.
                    self._wakeup.clear()
                    batch = await self._database.read_batch(
                        self._generation_id,
                        self._cursor,
                    )
                    if batch is None:
                        raise ReaderGenerationNotFound

                for event in self._render_chunks(batch.chunks):
                    yield event

                state = batch.state
                batch = None
                if state.status != "running":
                    # Emit terminal state only after every row through final_seq
                    # has been delivered.
                    if (
                        state.final_seq is not None
                        and self._cursor >= state.final_seq
                    ):
                        terminal_sent = True
                        yield _terminal_sse(state)
                        return
                    continue

                try:
                    await asyncio.wait_for(
                        self._wakeup.wait(),
                        timeout=self._heartbeat_s,
                    )
                except TimeoutError:
                    yield ": hb\n\n"
        except (asyncio.CancelledError, GeneratorExit):
            client_abort = not terminal_sent
            raise
        finally:
            await self.close(client_abort=client_abort)
