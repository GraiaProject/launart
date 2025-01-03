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

    def __init__(self):
        self.tasks = []

    def flush(self):
        if self.main is not None:
            self.main.cancel()

    def stop(self):
        self._stop = True
        self.flush()

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
            self.main = asyncio.create_task(asyncio.wait(self.tasks))
            try:
                return await self.main
            except asyncio.CancelledError:
                if self._stop:
                    self.main = None
                    return
