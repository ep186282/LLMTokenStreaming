from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import UUID


logger = logging.getLogger("uvicorn.error")
RunnerFactory = Callable[[asyncio.Event], Awaitable[None]]


@dataclass(slots=True)
class _Entry:
    task: asyncio.Task[None]
    cancel_requested: asyncio.Event


class GenerationTaskRegistry:
    def __init__(self) -> None:
        self._entries: dict[UUID, _Entry] = {}
        self._lock = asyncio.Lock()
        self._shutting_down = False

    async def ensure_started(
        self,
        generation_id: UUID,
        runner_factory: RunnerFactory,
    ) -> bool:
        async with self._lock:
            if self._shutting_down:
                return False
            current = self._entries.get(generation_id)
            if current is not None and not current.task.done():
                return False

            cancel_requested = asyncio.Event()
            task = asyncio.create_task(
                runner_factory(cancel_requested),
                name=f"generation-{generation_id}",
            )
            self._entries[generation_id] = _Entry(task, cancel_requested)
            task.add_done_callback(
                lambda completed: self._task_done(generation_id, completed)
            )
            return True

    def _task_done(
        self,
        generation_id: UUID,
        task: asyncio.Task[None],
    ) -> None:
        current = self._entries.get(generation_id)
        if current is not None and current.task is task:
            self._entries.pop(generation_id, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "generation task crashed gen=%s",
                str(generation_id)[:8],
                exc_info=(type(error), error, error.__traceback__),
            )

    async def signal_cancel(self, generation_id: UUID) -> bool:
        async with self._lock:
            current = self._entries.get(generation_id)
            if current is None or current.task.done():
                return False
            current.cancel_requested.set()
            return True

    async def shutdown(self) -> None:
        async with self._lock:
            self._shutting_down = True
            tasks = [entry.task for entry in self._entries.values()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def active_count(self) -> int:
        async with self._lock:
            return sum(
                not entry.task.done() for entry in self._entries.values()
            )
