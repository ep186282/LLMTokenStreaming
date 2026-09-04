from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from server.config import Settings
from server.providers import OpenRouterProvider, ProviderError


def test_settings_defaults() -> None:
    settings = Settings.from_env(
        {"OPENROUTER_API_KEY": "test-key"}
    )

    assert settings.database_url == (
        "postgres://postgres:postgres@localhost:5432/token_streamer"
    )
    assert settings.model == "google/gemini-3.5-flash-lite"
    assert settings.openrouter_api_key == "test-key"
    assert settings.openrouter_base_url == "https://openrouter.ai/api/v1"
    assert settings.flush_max_deltas == 1
    assert settings.flush_max_ms == 8
    assert settings.snapshot_threshold == 50
    assert settings.heartbeat_s == 15
    assert settings.client_origin == "http://localhost:5173"
    assert settings.client_origins == (
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    )


@pytest.mark.parametrize(
    "environment",
    [
        {},
        {"OPENROUTER_API_KEY": "test-key", "MODEL": ""},
        {"OPENROUTER_API_KEY": "test-key", "FLUSH_MAX_DELTAS": "0"},
        {"OPENROUTER_API_KEY": "test-key", "CLIENT_ORIGIN": ""},
    ],
)
def test_settings_reject_invalid_values(
    environment: dict[str, str],
) -> None:
    with pytest.raises(ValueError):
        Settings.from_env(environment)


class FakeChatStream:
    def __init__(self, events: list[Any]) -> None:
        self._events = iter(events)
        self.closed = False

    def __aiter__(self) -> FakeChatStream:
        return self

    async def __anext__(self) -> Any:
        try:
            return next(self._events)
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_openrouter_provider_maps_chat_stream_events() -> None:
    provider = OpenRouterProvider(
        "test-key",
        base_url="https://openrouter.ai/api/v1",
    )
    stream = FakeChatStream(
        [
            SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        delta=SimpleNamespace(content="hello"),
                        finish_reason=None,
                    )
                ],
                usage=None,
            ),
            SimpleNamespace(
                choices=[
                    SimpleNamespace(
                        delta=SimpleNamespace(content=None),
                        finish_reason="stop",
                    )
                ],
                usage={"output_tokens": 1},
            ),
        ]
    )

    async def create(**arguments: Any) -> FakeChatStream:
        assert arguments["messages"] == [
            {"role": "user", "content": "hello"}
        ]
        assert arguments["stream"] is True
        return stream

    provider._client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )
    events = [
        event
        async for event in provider.stream(
            model="openai/gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
        )
    ]

    assert events == [
        {"type": "text.delta", "text": "hello"},
        {
            "type": "provider.completed",
            "finish_reason": "stop",
            "usage": {"output_tokens": 1},
        },
    ]
    assert stream.closed


@pytest.mark.asyncio
async def test_openrouter_provider_redacts_api_key_from_errors() -> None:
    provider = OpenRouterProvider(
        "private-test-key",
        base_url="https://openrouter.ai/api/v1",
    )

    async def create(**arguments: Any) -> None:
        del arguments
        raise RuntimeError("request used private-test-key")

    provider._client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=create))
    )

    with pytest.raises(ProviderError) as captured:
        async for _ in provider.stream(
            model="openai/gpt-4o-mini",
            messages=[{"role": "user", "content": "hello"}],
        ):
            pass

    assert "private-test-key" not in str(captured.value)
    assert "[redacted]" in str(captured.value)
