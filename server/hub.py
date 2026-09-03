from __future__ import annotations

import asyncio
from collections import defaultdict
from uuid import UUID


class WakeupHub:
    def __init__(self) -> None:
        self._subscribers: dict[UUID, set[asyncio.Event]] = defaultdict(set)
        self._lock = asyncio.Lock()

    async def register(self, generation_id: UUID) -> asyncio.Event:
        event = asyncio.Event()
        async with self._lock:
            self._subscribers[generation_id].add(event)
        return event

    async def unregister(
        self, generation_id: UUID, event: asyncio.Event
    ) -> None:
        async with self._lock:
            subscribers = self._subscribers.get(generation_id)
            if subscribers is None:
                return
            subscribers.discard(event)
            if not subscribers:
                self._subscribers.pop(generation_id, None)

    async def publish(self, generation_id: UUID) -> None:
        async with self._lock:
            subscribers = tuple(self._subscribers.get(generation_id, ()))
        for event in subscribers:
            event.set()

    async def subscriber_count(self, generation_id: UUID) -> int:
        async with self._lock:
            return len(self._subscribers.get(generation_id, ()))
