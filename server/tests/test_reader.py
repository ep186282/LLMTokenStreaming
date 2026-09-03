from __future__ import annotations

from typing import cast
from uuid import UUID, uuid4

import pytest

from server.database import Database, ReadBatch, ReadState, StoredChunk
from server.hub import WakeupHub
from server.reader import CursorAhead, ReaderSession


class FakeReaderDatabase:
    def __init__(
        self,
        *,
        hub: WakeupHub,
        generation_id: UUID,
        batch: ReadBatch,
    ) -> None:
        self.hub = hub
        self.generation_id = generation_id
        self.batch = batch
        self.read_cursors: list[int] = []

    async def read_batch(
        self,
        generation_id: UUID,
        cursor: int,
    ) -> ReadBatch:
        assert generation_id == self.generation_id
        assert await self.hub.subscriber_count(generation_id) == 1
        self.read_cursors.append(cursor)
        return ReadBatch(
            state=self.batch.state,
            chunks=[
                chunk for chunk in self.batch.chunks if chunk.seq > cursor
            ],
        )


@pytest.mark.asyncio
async def test_reader_snapshots_backlog_then_emits_terminal() -> None:
    generation_id = uuid4()
    hub = WakeupHub()
    batch = ReadBatch(
        state=ReadState(
            status="completed",
            current_seq=3,
            final_seq=3,
            finish_reason="stop",
            usage={"output_tokens": 3},
            error=None,
        ),
        chunks=[
            StoredChunk(
                seq=sequence,
                event={"type": "text.delta", "text": str(sequence)},
            )
            for sequence in range(1, 4)
        ],
    )
    database = FakeReaderDatabase(
        hub=hub,
        generation_id=generation_id,
        batch=batch,
    )
    reader = await ReaderSession.open(
        database=cast(Database, database),
        hub=hub,
        generation_id=generation_id,
        cursor=0,
        snapshot_threshold=2,
        heartbeat_s=1,
    )

    events = [event async for event in reader.events()]

    assert len(events) == 2
    assert "id: 3\nevent: snapshot\n" in events[0]
    assert '"seq":1' in events[0]
    assert "id: 3\nevent: done\n" in events[1]
    assert '"finish_reason":"stop"' in events[1]
    assert await hub.subscriber_count(generation_id) == 0


@pytest.mark.asyncio
async def test_reader_rejects_cursor_past_current_sequence() -> None:
    generation_id = uuid4()
    hub = WakeupHub()
    database = FakeReaderDatabase(
        hub=hub,
        generation_id=generation_id,
        batch=ReadBatch(
            state=ReadState(
                status="running",
                current_seq=2,
                final_seq=None,
                finish_reason=None,
                usage=None,
                error=None,
            ),
            chunks=[],
        ),
    )

    with pytest.raises(CursorAhead) as captured:
        await ReaderSession.open(
            database=cast(Database, database),
            hub=hub,
            generation_id=generation_id,
            cursor=3,
            snapshot_threshold=50,
            heartbeat_s=1,
        )

    assert captured.value.current_seq == 2
    assert await hub.subscriber_count(generation_id) == 0
