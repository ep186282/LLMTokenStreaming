from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest

from server.app import create_app
from server.config import Settings


def make_settings() -> Settings:
    return Settings(
        database_url="postgres://unused",
        openrouter_api_key="test-key",
        model="openai/gpt-4o-mini",
        openrouter_base_url="https://openrouter.ai/api/v1",
        flush_max_deltas=8,
        flush_max_ms=20,
        snapshot_threshold=50,
        heartbeat_s=15,
        client_origin="http://localhost:5173",
    )


class FakeCreateDatabase:
    def __init__(self) -> None:
        self.generation_id = uuid4()
        self.calls = 0

    async def create_or_get_generation(
        self,
        *,
        idempotency_key: UUID,
        model: str,
        request: dict[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        del idempotency_key
        self.calls += 1
        return (
            {
                "id": self.generation_id,
                "status": "running",
                "model": model,
                "request": request,
                "cancel_requested": False,
            },
            self.calls == 1,
        )


class FakeTasks:
    def __init__(self) -> None:
        self.starts = 0

    async def ensure_started(self, generation_id: UUID, factory: Any) -> bool:
        del generation_id, factory
        self.starts += 1
        return self.starts == 1


@pytest.mark.asyncio
async def test_create_returns_201_then_200_with_stable_identity() -> None:
    settings = make_settings()
    application = create_app(settings)
    database = FakeCreateDatabase()
    tasks = FakeTasks()
    application.state.services = SimpleNamespace(
        settings=settings,
        database=database,
        tasks=tasks,
        runner=SimpleNamespace(run=None),
    )
    transport = httpx.ASGITransport(app=application)
    key = str(uuid4())

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        first = await client.post(
            "/generations",
            headers={"Idempotency-Key": key},
            json={"messages": [{"role": "user", "content": "hello"}]},
        )
        second = await client.post(
            "/generations",
            headers={"Idempotency-Key": key},
            json={"messages": [{"role": "user", "content": "hello"}]},
        )

    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json() == second.json()
    assert first.json()["generation_id"] == str(database.generation_id)


@pytest.mark.asyncio
async def test_cors_allows_client_stream_request_headers() -> None:
    application = create_app(make_settings())
    transport = httpx.ASGITransport(app=application)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        response = await client.options(
            f"/generations/{uuid4()}/events",
            headers={
                "Origin": "http://localhost:5173",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "cache-control",
            },
        )

    assert response.status_code == 200
    assert (
        response.headers["access-control-allow-origin"]
        == "http://localhost:5173"
    )


@pytest.mark.asyncio
async def test_cors_allows_loopback_origin() -> None:
    application = create_app(make_settings())
    transport = httpx.ASGITransport(app=application)

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://test",
    ) as client:
        response = await client.options(
            "/generations",
            headers={
                "Origin": "http://127.0.0.1:5173",
                "Access-Control-Request-Method": "POST",
                "Access-Control-Request-Headers": "content-type,idempotency-key",
            },
        )

    assert response.status_code == 200
    assert (
        response.headers["access-control-allow-origin"]
        == "http://127.0.0.1:5173"
    )
