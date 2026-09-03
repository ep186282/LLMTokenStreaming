from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from server.config import Settings
from server.database import Database, TerminalStatus
from server.hub import WakeupHub
from server.providers import Provider, ProviderEvent


logger = logging.getLogger("uvicorn.error")

_RECONCILE_RETRY_INITIAL_S = 0.1
_RECONCILE_RETRY_MAX_S = 5.0


@dataclass(slots=True)
class _WriteState:
    next_seq: int
    committed_seq: int
    pending: list[tuple[int, dict[str, Any]]] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class _Outcome:
    status: TerminalStatus
    finish_reason: str | None = None
    usage: dict[str, Any] | None = None
    error: str | None = None


@dataclass(slots=True)
class _StreamControl:
    next_event: asyncio.Task[ProviderEvent] | None = None
    cancel: asyncio.Task[bool] | None = None


class GenerationRunner:
    def __init__(
        self,
        *,
        database: Database,
        provider: Provider,
        hub: WakeupHub,
        settings: Settings,
    ) -> None:
        self._database = database
        self._provider = provider
        self._hub = hub
        self._flush_max_deltas = settings.flush_max_deltas
        self._flush_interval = settings.flush_max_ms / 1000.0
        self._api_key = settings.openrouter_api_key

    def _safe_error(self, error: BaseException) -> str:
        message = str(error)
        if self._api_key:
            message = message.replace(self._api_key, "[redacted]")
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
        return f"{type(error).__name__}: {message}"[:4000]

    async def _flush(
        self,
        generation_id: UUID,
        state: _WriteState,
    ) -> None:
        if not state.pending:
            return
        batch = list(state.pending)
        await self._database.insert_chunks(generation_id, batch)
        state.pending.clear()
        state.committed_seq = batch[-1][0]
        logger.debug(
            "gen=%s seq=%d flushed",
            str(generation_id)[:8],
            state.committed_seq,
        )
        # Readers are woken only after every row in this batch is committed.
        await self._hub.publish(generation_id)

    async def _flush_during_shutdown(
        self,
        generation_id: UUID,
        state: _WriteState,
    ) -> None:
        if not state.pending:
            return

        progress = await self._database.get_progress(generation_id)
        if progress is None:
            return
        last_pending_seq = state.pending[-1][0]
        if progress.current_seq >= last_pending_seq:
            state.pending.clear()
            state.committed_seq = last_pending_seq
            return
        await self._flush(generation_id, state)

    async def _close_stream(
        self,
        stream: Any,
        next_event_task: asyncio.Task[ProviderEvent] | None,
        cancel_task: asyncio.Task[bool] | None,
    ) -> None:
        tasks = [
            task
            for task in (next_event_task, cancel_task)
            if task is not None
        ]
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        close = getattr(stream, "aclose", None)
        if callable(close):
            try:
                await close()
            except Exception:
                logger.debug("provider stream close failed")

    async def _drive(
        self,
        generation_id: UUID,
        stream: Any,
        cancel_requested: asyncio.Event,
        state: _WriteState,
        control: _StreamControl,
    ) -> _Outcome:
        iterator = stream.__aiter__()
        control.next_event = asyncio.create_task(
            anext(iterator)
        )
        control.cancel = asyncio.create_task(cancel_requested.wait())
        flush_deadline: float | None = None
        finish_reason = "stop"
        usage: dict[str, Any] | None = None
        loop = asyncio.get_running_loop()

        while True:
            if control.next_event is None or control.cancel is None:
                raise RuntimeError("stream control lost an active task")
            timeout = None
            if state.pending and flush_deadline is not None:
                timeout = max(0.0, flush_deadline - loop.time())

            # The timeout flushes buffered deltas while the provider is quiet.
            done, _ = await asyncio.wait(
                {control.next_event, control.cancel},
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if control.cancel in done:
                return _Outcome(
                    status="cancelled",
                    finish_reason="cancelled",
                )

            if (
                state.pending
                and flush_deadline is not None
                and loop.time() >= flush_deadline
            ):
                await self._flush(generation_id, state)
                flush_deadline = None

            if control.next_event not in done:
                continue

            completed_task = control.next_event
            control.next_event = None
            try:
                event = completed_task.result()
            except StopAsyncIteration:
                return _Outcome(
                    status="completed",
                    finish_reason=finish_reason,
                    usage=usage,
                )

            # Keep one provider read in flight while this batch is persisted so
            # database latency does not delay the next delta.
            control.next_event = asyncio.create_task(anext(iterator))

            if event["type"] == "text.delta":
                state.next_seq += 1
                state.pending.append((state.next_seq, event))
                if flush_deadline is None:
                    flush_deadline = loop.time() + self._flush_interval
                if len(state.pending) >= self._flush_max_deltas:
                    await self._flush(generation_id, state)
                    flush_deadline = None
            else:
                finish_reason = event["finish_reason"]
                usage = event.get("usage")

    async def _reconcile_crash(
        self,
        generation_id: UUID,
        error_text: str,
    ) -> None:
        retry_s = _RECONCILE_RETRY_INITIAL_S
        while True:
            try:
                progress = await self._database.get_progress(generation_id)
                if progress is None:
                    return
                if progress.status != "running":
                    await self._hub.publish(generation_id)
                    return

                terminal_status = await self._database.finish_generation(
                    generation_id,
                    desired_status="failed",
                    final_seq=progress.current_seq,
                    error=error_text,
                )
                if terminal_status is not None:
                    await self._hub.publish(generation_id)
                    return
            except Exception:
                logger.exception(
                    "terminal reconciliation failed gen=%s retry_s=%.1f",
                    str(generation_id)[:8],
                    retry_s,
                )

            # Keeping this task alive preserves registry ownership, so a
            # duplicate start cannot launch a second provider stream.
            await asyncio.sleep(retry_s)
            retry_s = min(retry_s * 2, _RECONCILE_RETRY_MAX_S)

    async def run(
        self,
        generation_id: UUID,
        *,
        model: str,
        messages: list[dict[str, Any]],
        cancel_requested: asyncio.Event,
    ) -> None:
        try:
            await self._run_once(
                generation_id,
                model=model,
                messages=messages,
                cancel_requested=cancel_requested,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            error_text = self._safe_error(error)
            logger.exception(
                "generation writer crashed; retaining ownership gen=%s",
                str(generation_id)[:8],
            )
            await self._reconcile_crash(generation_id, error_text)

    async def _run_once(
        self,
        generation_id: UUID,
        *,
        model: str,
        messages: list[dict[str, Any]],
        cancel_requested: asyncio.Event,
    ) -> None:
        progress = await self._database.get_progress(generation_id)
        if progress is None or progress.status != "running":
            return
        if progress.cancel_requested:
            cancel_requested.set()

        state = _WriteState(
            next_seq=progress.current_seq,
            committed_seq=progress.current_seq,
        )
        stream = self._provider.stream(model=model, messages=messages)
        control = _StreamControl()

        try:
            outcome = await self._drive(
                generation_id,
                stream,
                cancel_requested,
                state,
                control,
            )
            await self._close_stream(
                stream,
                control.next_event,
                control.cancel,
            )
            control.next_event = None
            control.cancel = None
            await self._flush(generation_id, state)
            # Terminal state never names a sequence that is still buffered.
            terminal_status = await self._database.finish_generation(
                generation_id,
                desired_status=outcome.status,
                final_seq=state.committed_seq,
                finish_reason=outcome.finish_reason,
                usage=outcome.usage,
                error=outcome.error,
            )
            if terminal_status is not None:
                await self._hub.publish(generation_id)
        except asyncio.CancelledError:
            current_task = asyncio.current_task()
            if current_task is not None:
                current_task.uncancel()
            await self._close_stream(
                stream,
                control.next_event,
                control.cancel,
            )
            try:
                await self._flush_during_shutdown(generation_id, state)
            except Exception:
                logger.exception(
                    "shutdown flush failed gen=%s",
                    str(generation_id)[:8],
                )
            raise
        except Exception as error:
            await self._close_stream(
                stream,
                control.next_event,
                control.cancel,
            )
            error_text = self._safe_error(error)
            try:
                await self._flush(generation_id, state)
            except Exception as flush_error:
                logger.exception(
                    "partial flush failed gen=%s",
                    str(generation_id)[:8],
                )
                error_text = (
                    f"{error_text}; partial flush failed: "
                    f"{self._safe_error(flush_error)}"
                )[:4000]

            try:
                # A commit can succeed even when its acknowledgement is lost.
                progress = await self._database.get_progress(generation_id)
                if progress is not None:
                    state.committed_seq = max(
                        state.committed_seq,
                        progress.current_seq,
                    )
                terminal_status = await self._database.finish_generation(
                    generation_id,
                    desired_status="failed",
                    final_seq=state.committed_seq,
                    error=error_text,
                )
            except Exception:
                logger.exception(
                    "failed to persist terminal state gen=%s",
                    str(generation_id)[:8],
                )
                await self._reconcile_crash(generation_id, error_text)
                return

            logger.warning(
                "generation failed gen=%s error=%s",
                str(generation_id)[:8],
                error_text,
            )
            if terminal_status is not None:
                await self._hub.publish(generation_id)
