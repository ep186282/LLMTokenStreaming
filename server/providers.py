from __future__ import annotations

import asyncio
import inspect
import re
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Any, Literal, NotRequired, Protocol, TypedDict

from openai import AsyncOpenAI

from server.config import Settings


class TextDelta(TypedDict):
    type: Literal["text.delta"]
    text: str


class ProviderCompleted(TypedDict):
    type: Literal["provider.completed"]
    finish_reason: str
    usage: NotRequired[dict[str, Any] | None]


ProviderEvent = TextDelta | ProviderCompleted


class ProviderError(Exception):
    pass


class Provider(Protocol):
    def stream(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ProviderEvent]:
        ...


def _as_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        dumped = model_dump(mode="json", exclude_none=True)
        return dumped if isinstance(dumped, dict) else None
    return None


class OpenRouterProvider:
    def __init__(self, api_key: str, *, base_url: str) -> None:
        self._api_key = api_key
        self._client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url,
            default_headers={
                "HTTP-Referer": "http://localhost:5173",
                "X-Title": "Resilient LLM Token Streaming",
            },
        )

    def _safe_error(self, error: BaseException) -> str:
        message = str(error).replace(self._api_key, "[redacted]")
        message = re.sub(
            r"(?i)\bbearer\s+\S+",
            "Bearer [redacted]",
            message,
        )
        message = re.sub(
            r"\bsk-[A-Za-z0-9_-]{8,}",
            "[redacted]",
            message,
        )
        return f"{type(error).__name__}: {message}"[:2000]

    async def stream(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
    ) -> AsyncIterator[ProviderEvent]:
        stream: Any = None
        finish_reason = "stop"
        usage: dict[str, Any] | None = None
        try:
            stream = await self._client.chat.completions.create(
                model=model,
                messages=messages,
                stream=True,
                stream_options={"include_usage": True},
            )
            async for chunk in stream:
                chunk_usage = _as_dict(getattr(chunk, "usage", None))
                if chunk_usage:
                    usage = chunk_usage
                choices = getattr(chunk, "choices", None) or []
                if not choices:
                    continue
                choice = choices[0]
                delta = getattr(choice, "delta", None)
                content = getattr(delta, "content", None) if delta is not None else None
                if content:
                    yield {"type": "text.delta", "text": str(content)}
                reason = getattr(choice, "finish_reason", None)
                if reason:
                    finish_reason = str(reason)

            yield {
                "type": "provider.completed",
                "finish_reason": finish_reason,
                "usage": usage,
            }
        except asyncio.CancelledError:
            raise
        except ProviderError:
            raise
        except Exception as exc:
            raise ProviderError(self._safe_error(exc)) from None
        finally:
            close = getattr(stream, "close", None)
            if callable(close):
                with suppress(Exception):
                    result = close()
                    if inspect.isawaitable(result):
                        await result


def build_provider(settings: Settings) -> Provider:
    return OpenRouterProvider(
        settings.openrouter_api_key,
        base_url=settings.openrouter_base_url,
    )
