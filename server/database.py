from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

import asyncpg

from server.models import GenerationStatus


class GenerationNotFound(Exception):
    pass


class IdempotencyConflict(Exception):
    pass


@dataclass(frozen=True, slots=True)
class GenerationProgress:
    status: GenerationStatus
    current_seq: int
    cancel_requested: bool


@dataclass(frozen=True, slots=True)
class ReadState:
    status: GenerationStatus
    current_seq: int
    final_seq: int | None
    finish_reason: str | None
    usage: dict[str, Any] | None
    error: str | None


@dataclass(frozen=True, slots=True)
class StoredChunk:
    seq: int
    event: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ReadBatch:
    state: ReadState
    chunks: list[StoredChunk]


TerminalStatus = Literal["completed", "cancelled", "failed"]


class Database:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url
        self._pool: asyncpg.Pool | None = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("database pool is not open")
        return self._pool

    @staticmethod
    async def _initialize_connection(connection: asyncpg.Connection) -> None:
        await connection.set_type_codec(
            "json",
            schema="pg_catalog",
            encoder=json.dumps,
            decoder=json.loads,
            format="text",
        )
        await connection.set_type_codec(
            "jsonb",
            schema="pg_catalog",
            encoder=json.dumps,
            decoder=json.loads,
            format="text",
        )

    async def open(self) -> int:
        self._pool = await asyncpg.create_pool(
            self._database_url,
            min_size=1,
            max_size=10,
            command_timeout=30,
            init=self._initialize_connection,
        )
        try:
            schema = Path(__file__).with_name("schema.sql").read_text(
                encoding="utf-8"
            )
            async with self.pool.acquire() as connection:
                await connection.execute(schema)
                rows = await connection.fetch(
                    """
                    UPDATE generations AS g
                    SET status = 'interrupted',
                        final_seq = COALESCE(
                            (
                                SELECT MAX(c.seq)
                                FROM chunks AS c
                                WHERE c.generation_id = g.id
                            ),
                            0
                        ),
                        finish_reason = NULL,
                        usage = NULL,
                        error = 'server_restart',
                        updated_at = NOW(),
                        finished_at = NOW()
                    WHERE g.status = 'running'
                    RETURNING g.id
                    """
                )
            return len(rows)
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def healthcheck(self) -> None:
        await self.pool.fetchval("SELECT 1")

    async def create_or_get_generation(
        self,
        *,
        idempotency_key: UUID,
        model: str,
        request: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        generation_id = uuid4()
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    INSERT INTO generations (
                        id,
                        idempotency_key,
                        status,
                        model,
                        request
                    )
                    VALUES ($1, $2, 'running', $3, $4)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING id, status, model, request, cancel_requested
                    """,
                    generation_id,
                    idempotency_key,
                    model,
                    request,
                )
                created = row is not None
                if row is None:
                    row = await connection.fetchrow(
                        """
                        SELECT id, status, model, request, cancel_requested
                        FROM generations
                        WHERE idempotency_key = $1
                        """,
                        idempotency_key,
                    )
                if row is None:
                    raise RuntimeError("idempotency conflict row disappeared")
                if row["model"] != model or row["request"] != request:
                    raise IdempotencyConflict
                return dict(row), created

    async def get_generation_view(
        self, generation_id: UUID
    ) -> dict[str, Any] | None:
        row = await self.pool.fetchrow(
            """
            SELECT
                g.id,
                g.status,
                COALESCE(
                    (
                        SELECT MAX(c.seq)
                        FROM chunks AS c
                        WHERE c.generation_id = g.id
                    ),
                    0
                )::INTEGER AS seq,
                g.final_seq,
                COALESCE(
                    (
                        SELECT STRING_AGG(c.event ->> 'text', '' ORDER BY c.seq)
                        FROM chunks AS c
                        WHERE c.generation_id = g.id
                          AND c.event ->> 'type' = 'text.delta'
                    ),
                    ''
                ) AS text,
                g.finish_reason,
                g.usage,
                g.error,
                g.cancel_requested
            FROM generations AS g
            WHERE g.id = $1
            """,
            generation_id,
        )
        return dict(row) if row is not None else None

    async def get_progress(
        self, generation_id: UUID
    ) -> GenerationProgress | None:
        row = await self.pool.fetchrow(
            """
            SELECT
                g.status,
                COALESCE(
                    (
                        SELECT MAX(c.seq)
                        FROM chunks AS c
                        WHERE c.generation_id = g.id
                    ),
                    0
                )::INTEGER AS current_seq,
                g.cancel_requested
            FROM generations AS g
            WHERE g.id = $1
            """,
            generation_id,
        )
        if row is None:
            return None
        return GenerationProgress(
            status=row["status"],
            current_seq=row["current_seq"],
            cancel_requested=row["cancel_requested"],
        )

    async def insert_chunks(
        self,
        generation_id: UUID,
        chunks: list[tuple[int, dict[str, Any]]],
    ) -> None:
        if not chunks:
            return
        rows = [
            (generation_id, sequence, event)
            for sequence, event in chunks
        ]
        statement = """
            INSERT INTO chunks (generation_id, seq, event)
            VALUES ($1, $2, $3)
            ON CONFLICT (generation_id, seq) DO NOTHING
            """
        async with self.pool.acquire() as connection:
            if len(rows) == 1:
                await connection.execute(statement, *rows[0])
                return
            async with connection.transaction():
                await connection.executemany(statement, rows)

    async def finish_generation(
        self,
        generation_id: UUID,
        *,
        desired_status: TerminalStatus,
        final_seq: int,
        finish_reason: str | None = None,
        usage: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> TerminalStatus | None:
        async with self.pool.acquire() as connection:
            if desired_status == "cancelled":
                row = await connection.fetchrow(
                    """
                    UPDATE generations
                    SET status = 'cancelled',
                        final_seq = $2,
                        finish_reason = 'cancelled',
                        usage = NULL,
                        error = NULL,
                        updated_at = NOW(),
                        finished_at = NOW()
                    WHERE id = $1 AND status = 'running'
                    RETURNING status
                    """,
                    generation_id,
                    final_seq,
                )
            else:
                row = await connection.fetchrow(
                    """
                    UPDATE generations
                    SET status = $2,
                        final_seq = $3,
                        finish_reason = $4,
                        usage = $5,
                        error = $6,
                        updated_at = NOW(),
                        finished_at = NOW()
                    WHERE id = $1
                      AND status = 'running'
                      AND cancel_requested = FALSE
                    RETURNING status
                    """,
                    generation_id,
                    desired_status,
                    final_seq,
                    finish_reason,
                    usage,
                    error,
                )

            if row is not None:
                return row["status"]

            row = await connection.fetchrow(
                """
                UPDATE generations
                SET status = 'cancelled',
                    final_seq = $2,
                    finish_reason = 'cancelled',
                    usage = NULL,
                    error = NULL,
                    updated_at = NOW(),
                    finished_at = NOW()
                WHERE id = $1
                  AND status = 'running'
                  AND cancel_requested = TRUE
                RETURNING status
                """,
                generation_id,
                final_seq,
            )
            return row["status"] if row is not None else None

    async def request_cancel(self, generation_id: UUID) -> bool:
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    SELECT status
                    FROM generations
                    WHERE id = $1
                    FOR UPDATE
                    """,
                    generation_id,
                )
                if row is None:
                    raise GenerationNotFound
                if row["status"] != "running":
                    return False
                await connection.execute(
                    """
                    UPDATE generations
                    SET cancel_requested = TRUE,
                        updated_at = NOW()
                    WHERE id = $1
                    """,
                    generation_id,
                )
                return True

    async def read_batch(self, generation_id: UUID, cursor: int) -> ReadBatch | None:
        async with self.pool.acquire() as connection:
            # Read state and chunks from one snapshot so a terminal final_seq
            # cannot become visible without its committed rows.
            async with connection.transaction(
                isolation="repeatable_read",
                readonly=True,
            ):
                row = await connection.fetchrow(
                    """
                    SELECT
                        g.status,
                        COALESCE(
                            (
                                SELECT MAX(c.seq)
                                FROM chunks AS c
                                WHERE c.generation_id = g.id
                            ),
                            0
                        )::INTEGER AS current_seq,
                        g.final_seq,
                        g.finish_reason,
                        g.usage,
                        g.error
                    FROM generations AS g
                    WHERE g.id = $1
                    """,
                    generation_id,
                )
                if row is None:
                    return None
                chunk_rows = await connection.fetch(
                    """
                    SELECT seq, event
                    FROM chunks
                    WHERE generation_id = $1 AND seq > $2
                    ORDER BY seq
                    """,
                    generation_id,
                    cursor,
                )

        state = ReadState(
            status=row["status"],
            current_seq=row["current_seq"],
            final_seq=row["final_seq"],
            finish_reason=row["finish_reason"],
            usage=row["usage"],
            error=row["error"],
        )
        chunks = [
            StoredChunk(seq=chunk["seq"], event=chunk["event"])
            for chunk in chunk_rows
        ]
        return ReadBatch(state=state, chunks=chunks)
