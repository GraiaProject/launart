from __future__ import annotations

import asyncio
from contextlib import contextmanager
from typing import TYPE_CHECKING, Coroutine, Iterable, TypeVar

from loguru import logger
from typing_extensions import TypeAlias

if TYPE_CHECKING:
    from contextvars import ContextVar


_CoroutineLike: TypeAlias = "Coroutine | asyncio.Task"


def into_tasks(awaitables: Iterable[_CoroutineLike]) -> list[asyncio.Task]:
    return [i if isinstance(i, asyncio.Task) else asyncio.create_task(i) for i in awaitables]


async def unity(
    tasks: Iterable[_CoroutineLike],
    *,
    timeout: float | None = None,  # noqa: ASYNC109
    return_when: str = asyncio.ALL_COMPLETED,
):
    return await asyncio.wait(into_tasks(tasks), timeout=timeout, return_when=return_when)


async def any_completed(tasks: Iterable[_CoroutineLike]):
    done, pending = await unity(tasks, return_when=asyncio.FIRST_COMPLETED)
    return next(iter(done)), pending


async def oneof(*tasks: _CoroutineLike):
    return await any_completed(tasks)


def cancel_alive_tasks(loop: asyncio.AbstractEventLoop):
    to_cancel = asyncio.tasks.all_tasks(loop)
    if to_cancel:
        for tsk in to_cancel:
            tsk.cancel()
        loop.run_until_complete(asyncio.gather(*to_cancel, return_exceptions=True))

        for task in to_cancel:  # pragma: no cover
            # BELIEVE IN PSF
            if task.cancelled():
                continue
            if task.exception() is not None:
                logger.opt(exception=task.exception()).error(f"Unhandled exception when shutting down {task}:")


T = TypeVar("T")


@contextmanager
def cvar(ctx: ContextVar[T], val: T):
    token = ctx.set(val)
    try:
        yield val
    finally:
        ctx.reset(token)


class TaskGroup:
    tasks: list[asyncio.Task]
    main: asyncio.Task | None = None
    _stop: bool = False
    _notify: asyncio.Event

    def __init__(self):
        self.tasks = []
        self._notify = asyncio.Event()

    def flush(self):
        if self.main is not None:
            self._notify.set()

    def stop(self):
        self._stop = True
        self.flush()

    def spawn(self, task: asyncio.Task | Coroutine):
        task = asyncio.create_task(task) if asyncio.iscoroutine(task) else task
        self.tasks.append(task)

        self.flush()
        return task

    def update(self, tasks: Iterable[asyncio.Task | Coroutine]):
        tasks = [asyncio.create_task(task) if asyncio.iscoroutine(task) else task for task in tasks]
        self.tasks.extend(tasks)

        self.flush()
        return tasks

    def drop(self, tasks: Iterable[asyncio.Task]):
        for task in tasks:
            self.tasks.remove(task)

        self.flush()

    async def wait(self):
        while True:
            if not self.tasks:
                await self._notify.wait()
                self._notify.clear()

            self.main = asyncio.create_task(asyncio.wait(self.tasks))
            awaiting_notify = asyncio.create_task(self._notify.wait())

            await asyncio.wait([self.main, awaiting_notify], return_when=asyncio.FIRST_COMPLETED)

            if awaiting_notify.done():
                self._notify.clear()
                if self._stop:
                    break

                continue

            await self.main
            return

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        await self.wait()

    def __await__(self):
        return self.wait().__await__()
