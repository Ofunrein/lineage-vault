from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

from ..models.events import LineageEvent


class IngestionPipeline:
    def __init__(self, handler: Callable[[LineageEvent], Awaitable[None]],
                 max_queue: int = 10_000, rate: float = 5000) -> None:
        self._handler = handler
        self._queue: asyncio.Queue[LineageEvent | None] = asyncio.Queue(maxsize=max_queue)
        self._max_queue = max_queue
        self._workers: list[asyncio.Task] = []
        self._dropped = 0

    @property
    def dropped(self) -> int:
        return self._dropped

    @property
    def max_queue(self) -> int:
        return self._max_queue

    async def start(self, n_workers: int = 4) -> None:
        self._workers = [asyncio.create_task(self._worker()) for _ in range(n_workers)]

    async def ingest(self, event: LineageEvent) -> bool:
        try:
            self._queue.put_nowait(event)
            return True
        except asyncio.QueueFull:
            self._dropped += 1
            return False

    async def _worker(self) -> None:
        while True:
            event = await self._queue.get()
            if event is None:
                break
            await self._handler(event)
            self._queue.task_done()

    async def stop(self) -> None:
        for _ in self._workers:
            await self._queue.put(None)
        await asyncio.gather(*self._workers)
